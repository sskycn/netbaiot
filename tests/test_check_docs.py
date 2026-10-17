import importlib.util
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("check_docs", Path(__file__).resolve().parents[1] / "scripts/check_docs.py")
docs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(docs)


class DocumentationChecks(unittest.TestCase):
    def test_unicode_duplicate_headings_and_fenced_examples(self):
        text = '# 中文 `Codec`：配置\n## Repeated heading\n## Repeated heading\n```md\n## Imaginary\n[x](missing.md)\n```\n<a id="explicit"></a>'
        self.assertEqual(docs.anchors(text), {'中文-codec配置', 'repeated-heading', 'repeated-heading-1', 'explicit'})
        self.assertEqual(list(docs.links(text)), [])

    def test_inline_images_references_titles_and_external_links(self):
        text = '[a](other.md#one "title") ![b](image.png)\n[ref]: <with%20space.md#two>\n[a](https://example.org)'
        self.assertEqual(list(docs.links(text)), [(1, 'other.md#one'), (1, 'image.png'), (2, 'with%20space.md#two'), (3, 'https://example.org')])

    def test_real_targets_fragments_and_escaped_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'README.md'
            source.write_text('# Local\n[a](with%20space.md#中文)\n[b](#local)\n[c](https://example.org/no-check)', encoding='utf-8')
            (root / 'with space.md').write_text('# 中文', encoding='utf-8')
            self.assertEqual(docs.check(root, [source]), (2, []))

    def test_missing_file_missing_anchor_and_outside_root_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'README.md'
            source.write_text('# Existing\n[a](missing.md)\n[b](#missing)\n[c](../outside.md)', encoding='utf-8')
            checked, failures = docs.check(root, [source])
            self.assertEqual(checked, 3)
            self.assertEqual(len(failures), 3)
            self.assertIn('missing anchor', failures[1])

    def test_inventory_preserves_historical_categories(self):
        root = Path('/repository')
        entries = docs.inventory(root, [root / 'docs/releases/v0.2.4.md', root / 'docs/migration/example.md', root / 'docs/configuration-fields.md'])
        self.assertEqual([e['status'] for e in entries['markdown']], ['historical evidence / index', 'compatibility guidance', 'generated reference'])


if __name__ == '__main__':
    unittest.main()
