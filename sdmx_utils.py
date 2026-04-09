"""Shared XML parsing utilities used by server.py and scripts."""

from xml.etree import ElementTree as ET


def tag_name(element: ET.Element) -> str:
    """Return the local (non-namespace) tag name of an XML element."""
    return element.tag.split("}")[-1]


def element_text(node: ET.Element, tag: str) -> str:
    """Return the stripped text of the first descendant with the given local tag name."""
    for elem in node.iter():
        if tag_name(elem) == tag and elem.text:
            text = elem.text.strip()
            if text:
                return text
    return ""
