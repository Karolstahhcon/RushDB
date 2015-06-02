#!/usr/bin/env python3
"""Each README_<LANG>.md carries the English README's code blocks verbatim,
the same links to repository files, and the language bar, so commands and
paths cannot drift between translations. Prose is reviewed by people."""
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parent.parent
english = (root / 'README.md').read_text()
code = re.findall(r'```[a-z]*\n.*?\n```', english, flags=re.S)

def local_links(text):
    """Links to repository files, without the language bar (each file leaves itself out)."""
    links = re.findall(r'\]\(((?!https?:)[^)#]+)', text)
    return sorted({link for link in links if not re.fullmatch(r'README(_[A-Z]{2})?\.md', link)})

translations = sorted(root.glob('README_*.md'))
languages = re.findall(r'\[[^\]]+\]\((README(?:_[A-Z]{2})?\.md)\)', english)

failures = []
if sorted(languages) != sorted(path.name for path in translations):
