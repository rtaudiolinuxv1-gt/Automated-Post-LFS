from __future__ import annotations

import ast


class SimpleYamlError(ValueError):
    pass


def load(text):
    parser = _Parser(text.splitlines())
    value, _ = parser.parse_block(0, 0)
    return value if value is not None else {}


def load_file(path):
    with open(path, "r", encoding="utf-8") as handle:
        return load(handle.read())


class _Parser:
    def __init__(self, lines):
        self.lines = lines

    def parse_block(self, start, indent):
        mapping = {}
        sequence = []
        mode = None
        index = start
        while index < len(self.lines):
            raw = self.lines[index]
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                index += 1
                continue

            current = len(raw) - len(raw.lstrip(" "))
            if current < indent:
                break
            if current > indent:
                raise SimpleYamlError("Invalid indentation near line %d" % (index + 1))

            token = raw[current:]
            if token.startswith("- "):
                if mode is None:
                    mode = "sequence"
                if mode != "sequence":
                    raise SimpleYamlError("Mixed mapping and list near line %d" % (index + 1))
                item_text = token[2:].strip()
                if not item_text:
                    child, index = self.parse_block(index + 1, indent + 2)
                    sequence.append(child)
                    continue
                if ":" in item_text and not item_text.startswith(("'", '"', "[", "{")):
                    key, value_text = item_text.split(":", 1)
                    entry = {}
                    if value_text.strip():
                        entry[key.strip()] = _parse_scalar(value_text.strip())
                        child, next_index = self._parse_child_mapping(index + 1, indent + 2)
                        entry.update(child)
                        sequence.append(entry)
                        index = next_index
                        continue
                    child, next_index = self.parse_block(index + 1, indent + 2)
                    entry[key.strip()] = child
                    sequence.append(entry)
                    index = next_index
                    continue
                sequence.append(_parse_scalar(item_text))
                index += 1
                continue

            if mode is None:
                mode = "mapping"
            if mode != "mapping":
                raise SimpleYamlError("Mixed mapping and list near line %d" % (index + 1))
            if ":" not in token:
                raise SimpleYamlError("Expected key/value near line %d" % (index + 1))
            key, value_text = token.split(":", 1)
            key = key.strip()
            value_text = value_text.strip()
            if value_text:
                mapping[key] = _parse_scalar(value_text)
                index += 1
                continue
            child, index = self.parse_block(index + 1, indent + 2)
            mapping[key] = child
        if mode == "sequence":
            return sequence, index
        return mapping, index

    def _parse_child_mapping(self, start, indent):
        if start >= len(self.lines):
            return {}, start
        raw = self.lines[start]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            return self.parse_block(start, indent)
        current = len(raw) - len(raw.lstrip(" "))
        if current < indent:
            return {}, start
        child, index = self.parse_block(start, indent)
        if not isinstance(child, dict):
            raise SimpleYamlError("Expected mapping child near line %d" % (start + 1))
        return child, index


def _parse_scalar(text):
    if text in ("null", "Null", "NULL", "~"):
        return None
    if text in ("true", "True", "TRUE", "yes", "on"):
        return True
    if text in ("false", "False", "FALSE", "no", "off"):
        return False
    if text.startswith(("'", '"')) and text.endswith(("'", '"')):
        return ast.literal_eval(text)
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    if text.startswith("{") and text.endswith("}"):
        return ast.literal_eval(text)
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text
