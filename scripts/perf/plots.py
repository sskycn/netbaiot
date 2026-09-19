#!/usr/bin/env python3
"""Optional offline charts (requires matplotlib); run after timed experiments."""
import json
import os
import pathlib
import statistics

os.environ.setdefault('MPLCONFIGDIR', '/tmp/netbaiot-capacity-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUT = pathlib.Path(__file__).resolve().parents[2] / 'docs/performance'
plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False,
                     'figure.facecolor': 'white', 'savefig.facecolor': 'white'})


def save(fig, name):
    fig.savefig(OUT / (name + '.png'), dpi=150, bbox_inches='tight')
    fig.savefig(OUT / (name + '.svg'), bbox_inches='tight')
    plt.close(fig)


def connections(evidence):
    fig, ax = plt.subplots(figsize=(9, 5), layout='constrained')
    for prefix, label, color in [('idle_plain_', 'Plain MQTT', '#0072B2'),
                                  ('idle_tls_cold_', 'Cold TLS', '#D55E00')]:
        xs, ys, low, high = [], [], [], []
        for n in [100, 1000, 2000]:
            values = [d['server_rss_kib']['median'] / 1024 for k, d in evidence.items()
                      if k.startswith(f'{prefix}{n}_r')]
            if values:
                mid = statistics.median(values)
                xs.append(n); ys.append(mid); low.append(mid - min(values)); high.append(max(values) - mid)
        ax.errorbar(xs, ys, yerr=[low, high], marker='o', capsize=4, color=color, label=label + ', 64 KiB frames')
        kind = 'plain' if prefix == 'idle_plain_' else 'tls'
        values = [d['server_rss_kib']['median'] / 1024 for k, d in evidence.items()
                  if k.startswith(f'idle_8k_{kind}_3400_r')]
        if values:
            mid = statistics.median(values)
            ax.errorbar([3400], [mid], yerr=[[mid - min(values)], [max(values) - mid]],
                        fmt='D', markersize=7, markerfacecolor='none', capsize=4,
                        color=color, label=label + ', 8 KiB frames')
    ax.set(xlabel='Concurrent MQTT connections', ylabel='Steady server RSS (MiB)',
           title='Measured idle memory: median of three runs, range bars')
    ax.grid(alpha=.2); ax.legend(loc='upper left', frameon=False)
    fig.text(.02, -.04, 'Different frame profiles are not a single capacity curve. RSS includes allocator/runtime effects.', fontsize=9)
    save(fig, 'capacity-connections')


def datasets():
    scales = []
    for n in [10000, 100000, 1000000]:
        path = OUT / f'dataset_{n}.json'
        if path.exists():
            scales.append((n, json.loads(path.read_text())))
    if not scales:
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), layout='constrained')
    for ax, queries in zip(axes, [
        ['quota_aggregation', 'command_quota', 'expired_commands'],
        ['dedup', 'outbox_claim', 'command_batch', 'ingress_retention', 'attempt_lookup'],
    ]):
        for name in queries:
            xs, ys, low, high = [], [], [], []
            for n, d in scales:
                q = d['queries'][name]; mid = q['median_ms']; runs = q['execution_ms']
                xs.append(n); ys.append(mid); low.append(mid - min(runs)); high.append(max(runs) - mid)
            ax.errorbar(xs, ys, yerr=[low, high], marker='o', capsize=3, label=name.replace('_', ' '))
        ax.set(xscale='log', yscale='log', xlabel='Rows per table', ylabel='EXPLAIN execution (ms)')
        ax.set_xticks([1e4, 1e5, 1e6], ['10K', '100K', '1M']); ax.grid(alpha=.2); ax.legend(frameon=False, fontsize=9)
    axes[0].set_title('Global scans'); axes[1].set_title('Lookup, claim and cleanup')
    fig.suptitle('Warm query plans: median and range of three measurements')
    fig.text(.01, -.045, 'Direct SQL fixtures bypass runtime quotas. This is query scaling, not accepted device capacity.', fontsize=9)
    save(fig, 'query-scaling')


def soak(evidence):
    path = OUT / 'soak_tls_2h.json'
    if not path.exists():
        return
    d = json.loads(path.read_text())
    if not d.get('ended_epoch'):
        return
    rows = d['samples']
    fig, axes = plt.subplots(4, 2, figsize=(12, 12), layout='constrained')

    def line(ax, section, key, label, scale=1, color=None):
        pairs = [(r['elapsed_s'] / 60, r[section][key] / scale) for r in rows if key in r.get(section, {})]
        if pairs:
            x, y = zip(*pairs); ax.plot(x, y, label=label, linewidth=1.1, color=color)

    line(axes[0, 0], 'server', 'rss_kib', 'RSS', 1024)
    axes[0, 0].set(title='Server RSS', ylabel='MiB')
    storage_size = axes[0, 1].twinx()
    line(axes[0, 1], 'postgres', 'rows', 'Retained ingress', 1000, '#0072B2')
    line(storage_size, 'postgres', 'db_bytes', 'Database size', 1048576, '#D55E00')
    axes[0, 1].set(title='Retained storage', ylabel='Thousands of rows')
    storage_size.set_ylabel('MiB', color='#D55E00')
    for key, label in [('netbaiot_registered_sessions', 'Live sessions'), ('netbaiot_runtime_alive_tasks', 'Tokio tasks')]:
        line(axes[1, 0], 'metrics', key, label)
    line(axes[1, 0], 'server', 'fds', 'FDs')
    axes[1, 0].set(title='Owned resources', ylabel='Count')
    line(axes[1, 1], 'metrics', 'netbaiot_ingress_inflight', 'Ingress in flight')
    line(axes[1, 1], 'metrics', 'netbaiot_queue_depth', 'Outbound count')
    axes[1, 1].set(title='Sampled in-memory work', ylabel='Count')
    outbox_age = axes[2, 0].twinx()
    windows = evidence.get('soak_tls_2h', {}).get('five_minute_windows', [])
    for ax, key, label, scale, color in [
        (axes[2, 0], 'outbox_pending', 'Max pending rows', 1, '#0072B2'),
        (outbox_age, 'oldest_outbox_ms', 'Max oldest age', 1000, '#D55E00'),
    ]:
        pairs = [(w['end_s'] / 60, w['postgres'][key]['max'] / scale)
                 for w in windows if w.get('postgres', {}).get(key)]
        if pairs:
            x, y = zip(*pairs); ax.plot(x, y, marker='.', label=label, color=color)
    axes[2, 0].set(title='Backlog: 5-min sampled maxima', ylabel='Rows')
    axes[2, 0].set_ylim(bottom=0)
    outbox_age.set_ylabel('Seconds', color='#D55E00')
    outbox_age.set_ylim(bottom=0)
    intervals = evidence.get('soak_tls_2h', {}).get('generator_intervals', [])
    pairs = [(r['end_s'] / 60, r['latency_interval_mean_ms']['application_ack']) for r in intervals
             if 'application_ack' in r['latency_interval_mean_ms']]
    if pairs:
        x, y = zip(*pairs); axes[2, 1].plot(x, y, linewidth=1.1, label='Interval mean ACK')
    store_key = 'successful_store_ms_floor_per_ingress_accept'
    pairs = [(r['end_s'] / 60, r[store_key]) for r in windows if store_key in r]
    if pairs:
        x, y = zip(*pairs); axes[2, 1].plot(x, y, linewidth=1.1, label='5-min store mean (ms floor)')
    axes[2, 1].set(title='Client receipt latency trend', ylabel='ms')
    for label in ['server', 'generator']:
        pairs = []
        for before, after in zip(rows, rows[1:]):
            a, b = before.get(label, {}), after.get(label, {})
            span = after['elapsed_s'] - before['elapsed_s']
            if span > 0 and 'cpu_seconds' in a and 'cpu_seconds' in b and a.get('pid') == b.get('pid'):
                pairs.append((after['elapsed_s'] / 60, max(0, b['cpu_seconds'] - a['cpu_seconds']) / span))
        if pairs:
            x, y = zip(*pairs); axes[3, 0].plot(x, y, linewidth=1, label=label)
    axes[3, 0].set(title='Separate process CPU', ylabel='Logical cores')
    for key, label in [('client_errors', 'Client errors'), ('ack_timeouts', 'ACK timeouts')]:
        pairs = [(r['end_s'] / 60, r['counter_deltas'].get(key, 0)) for r in intervals]
        if pairs:
            x, y = zip(*pairs); axes[3, 1].plot(x, y, linewidth=1, label=label)
    axes[3, 1].set(title='Client failures per report interval', ylabel='Count (normally 30 s)')
    for ax in axes.flat:
        ax.set_xlabel('Elapsed minutes'); ax.grid(alpha=.2); ax.legend(frameon=False, fontsize=8)
    for left, right in [(axes[0, 1], storage_size), (axes[2, 0], outbox_age)]:
        handles, labels = left.get_legend_handles_labels()
        other, titles = right.get_legend_handles_labels()
        left.legend(handles + other, labels + titles, frameon=False, fontsize=8, loc='upper left')
        right.spines['right'].set_visible(True)
        right.tick_params(axis='y', colors='#D55E00')
    fig.suptitle('Two-hour local TLS soak: sampled trends, not instantaneous maxima')
    save(fig, 'soak-trends')


if __name__ == '__main__':
    data = json.loads((OUT / 'resource-evidence.json').read_text())
    connections(data); datasets(); soak(data)
