"""Every block copied into a Fabric notebook equals its source in ingestion/.

A Fabric notebook cannot import the ingestion package, so each notebook that
needs the landing contract, the silver routine or batch discovery holds a
verbatim copy of the source block. This test reads files as text only, so it
runs without PySpark.
"""

from __future__ import annotations

import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FABRIC = REPO / "fabric"

# block name -> (source file, notebooks that must hold the block)
BLOCKS = {
    "landing-contract": ("ingestion/landing_contract.py", [
        "nb_landing_export_batch0", "nb_landing_gate_batch0", "nb_batch_discover",
        "nb_silver_apply_batch", "nb_silver_hash_baseline"]),
    "silver-routine": ("ingestion/silver_routine.py", ["nb_silver_apply_batch", "nb_silver_hash_baseline"]),
    "batch-discovery": ("ingestion/batch_discovery.py", ["nb_batch_discover"]),
}


def block(text, name):
    """The named block of a text, markers included, or None when absent."""
    begin, end = f"# >>> {name}:begin", f"# <<< {name}:end"
    if begin not in text:
        return None
    start = text.index(begin)
    return text[start:text.index(end, start) + len(end)]


def notebook_text(name):
    return (FABRIC / f"{name}.Notebook" / "notebook-content.py").read_text(encoding="utf-8")


class NotebookCopies(unittest.TestCase):
    def test_required_notebooks_hold_each_block_verbatim(self):
        for name, (source, notebooks) in BLOCKS.items():
            expected = block((REPO / source).read_text(encoding="utf-8"), name)
            self.assertIsNotNone(expected, f"{source} has no {name} block")
            for notebook in notebooks:
                with self.subTest(block=name, notebook=notebook):
                    self.assertEqual(block(notebook_text(notebook), name), expected,
                                     f"paste the {name} block from {source} into {notebook}")

    def test_no_other_notebook_holds_a_stale_copy(self):
        for name, (source, _) in BLOCKS.items():
            expected = block((REPO / source).read_text(encoding="utf-8"), name)
            for path in sorted(FABRIC.glob("*.Notebook/notebook-content.py")):
                found = block(path.read_text(encoding="utf-8"), name)
                if found is not None:
                    with self.subTest(block=name, notebook=path.parent.name):
                        self.assertEqual(found, expected, f"{path.parent.name} holds a stale {name} block")

    def test_each_block_appears_once(self):
        for name, (_, notebooks) in BLOCKS.items():
            for notebook in notebooks:
                with self.subTest(block=name, notebook=notebook):
                    self.assertEqual(notebook_text(notebook).count(f"# >>> {name}:begin"), 1)


if __name__ == "__main__":
    unittest.main()
