"""Read-only Linux process/cgroup/socket snapshots; ceilings are not memory use."""
import os
from pathlib import Path
import time
import re
import subprocess


def integers(path):
    result = {}
    try:
        for line in Path(path).read_text().splitlines():
            fields = line.replace(":", "").split()
            if len(fields) >= 2:
                try:
                    result[fields[0]] = int(fields[1]) * (1024 if fields[-1] == "kB" else 1)
                except ValueError:
                    pass
    except OSError:
        pass
    return result


def number(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def cgroup(pid):
    """Resolve this process's cgroup using mounts; v1 never masquerades as v2."""
    try:
        memberships = Path('/proc/%d/cgroup' % pid).read_text().splitlines()
        mounts = Path('/proc/%d/mountinfo' % pid).read_text().splitlines()
    except OSError:
        return None
    for line in memberships:
        _, controllers, group = line.split(':', 2)
        version = 2 if controllers == '' else 1
        if version == 1 and 'memory' not in controllers.split(','):
            continue
        for mount in mounts:
            before, after = mount.split(' - ', 1)
            fields, extra = before.split(), after.split()
            if extra[0] != ('cgroup2' if version == 2 else 'cgroup'):
                continue
            if version == 1 and 'memory' not in extra[-1].split(','):
                continue
            root, location = fields[3:5]
            relative = os.path.relpath(group, root)
            if relative.startswith('..'):
                continue
            directory = Path(location) / relative
            result = {'version': version, 'path': str(directory),
                      'scope': 'all processes in this cgroup, not server-only'}
            if version == 2:
                result.update(current=number(directory / 'memory.current'),
                              peak=number(directory / 'memory.peak'),
                              stat=integers(directory / 'memory.stat'),
                              events=integers(directory / 'memory.events'))
            else:
                result.update(usage_in_bytes=number(directory / 'memory.usage_in_bytes'),
                              max_usage_in_bytes=number(directory / 'memory.max_usage_in_bytes'),
                              tcp_usage_in_bytes=number(directory / 'memory.kmem.tcp.usage_in_bytes'),
                              kmem_usage_in_bytes=number(directory / 'memory.kmem.usage_in_bytes'),
                              stat=integers(directory / 'memory.stat'))
            return result
    return None


def socket_summary():
    result = {'scope': 'network namespace, includes local load clients and other processes'}
    try:
        for line in Path('/proc/net/sockstat').read_text().splitlines():
            fields = line.split()
            result[fields[0].rstrip(':')] = {
                fields[i]: int(fields[i + 1]) for i in range(1, len(fields) - 1, 2)
            }
        result['tcp_memory_bytes'] = result.get('TCP', {}).get('mem', 0) * os.sysconf('SC_PAGE_SIZE')
    except OSError:
        pass
    return result


def process(pid):
    if not Path('/proc/%d/status' % pid).exists():
        return None
    fields = Path('/proc/%d/stat' % pid).read_text().rsplit(')', 1)[1].split()
    fd_count = socket_fds = 0
    try:
        with os.scandir('/proc/%d/fd' % pid) as entries:
            for entry in entries:
                fd_count += 1
                try:
                    socket_fds += os.readlink(entry.path).startswith('socket:[')
                except OSError:
                    pass
    except OSError:
        pass
    return {'pid': pid, 'monotonic_seconds': time.monotonic(),
            'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
            'status_bytes': integers('/proc/%d/status' % pid),
            'smaps_rollup_bytes': integers('/proc/%d/smaps_rollup' % pid),
            'fd_count': fd_count, 'socket_fd_count': socket_fds,
            'cgroup': cgroup(pid), 'namespace_sockets': socket_summary()}


def cpu_percent(before, after):
    if not before or not after:
        return None
    elapsed = after['monotonic_seconds'] - before['monotonic_seconds']
    return 100 * (after['cpu_seconds'] - before['cpu_seconds']) / elapsed if elapsed > 0 else None


def tcp_listener_memory(port):
    """Stream inet_diag via ss; queue use and buffer ceilings stay separate.

    Includes sockets with this local port in the current network namespace;
    cgroup TCP accounting/slab remains separately reported, when available.
    """
    result = {'scope': 'established TCP sockets whose local port is %d' % port,
              'established': 0, 'recv_queue_bytes': 0, 'send_queue_bytes': 0,
              'skmem': {}, 'socket_buffers_are_ceilings': ['rb', 'tb']}
    try:
        child = subprocess.Popen(['ss', '-H', '-t', '-n', '-m', 'state', 'established',
                                  '( sport = :%d )' % port], stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, universal_newlines=True)
    except OSError:
        return None
    try:
        for line in child.stdout:
            fields = line.split()
            if fields and fields[0].isdigit() and len(fields) >= 4:
                result['established'] += 1
                result['recv_queue_bytes'] += int(fields[0])
                result['send_queue_bytes'] += int(fields[1])
            for name, value in re.findall(r'\b(r|rb|t|tb|f|w|o|bl|d)(\d+)', line):
                result['skmem'][name] = result['skmem'].get(name, 0) + int(value)
        child.wait(timeout=10)
        return result if child.returncode == 0 else None
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        child.stdout.close()
