"""Fast equality-only rule matching helpers."""

from __future__ import annotations

from collections import defaultdict
from operator import attrgetter
from typing import Any, Iterable, Literal, Mapping, Sequence


RecordMode = Literal["attr", "dict"]


class EqRuleLabeler:
    """Compiled equality matcher for labeling records.

    Rules are compiled once into hash indexes grouped by their field set. Runtime
    matching checks those indexes directly instead of scanning every rule for
    every record.
    """

    def __init__(
        self,
        rules: Sequence[Mapping[str, Any]],
        *,
        record_mode: RecordMode = "attr",
        multi_match: bool = True,
    ) -> None:
        if record_mode not in ("attr", "dict"):
            raise ValueError("record_mode must be 'attr' or 'dict'")
        self.record_mode = record_mode
        self.multi_match = bool(multi_match)
        self._indexes = self._compile(rules)
        self._accessors = {
            fields: self._build_accessor(fields) for fields in self._indexes
        }

    @property
    def field_sets(self) -> tuple[tuple[str, ...], ...]:
        """Field combinations that will be checked at runtime."""

        return tuple(self._indexes)

    @property
    def rule_count(self) -> int:
        """Number of configured rules after compilation."""

        return sum(
            len(labels) for index in self._indexes.values() for labels in index.values()
        )

    def match(self, record: Any) -> tuple[Any, ...]:
        """Return labels matching one record."""

        labels: list[Any] = []
        for fields, index in self._indexes.items():
            key = self._value_tuple(fields, record)
            if key is None:
                continue
            matched = index.get(key)
            if not matched:
                continue
            if not self.multi_match:
                return (matched[0],)
            labels.extend(matched)
        return tuple(labels)

    def match_many(self, records: Iterable[Any]) -> list[tuple[Any, ...]]:
        """Return labels for every record without mutating/copying records."""

        return [self.match(record) for record in records]

    @staticmethod
    def _compile(
        rules: Sequence[Mapping[str, Any]],
    ) -> dict[tuple[str, ...], dict[tuple[Any, ...], tuple[Any, ...]]]:
        grouped: dict[tuple[str, ...], dict[tuple[Any, ...], list[Any]]] = {}
        for index, rule in enumerate(rules):
            if not isinstance(rule, Mapping):
                raise TypeError(f"rule[{index}] must be a mapping")
            when = rule.get("when")
            if not isinstance(when, Mapping) or not when:
                raise ValueError(f"rule[{index}] requires non-empty 'when' mapping")
            if "label" not in rule:
                raise ValueError(f"rule[{index}] requires 'label'")
            pairs = sorted((str(field), value) for field, value in when.items())
            fields = tuple(field for field, _value in pairs)
            values = tuple(value for _field, value in pairs)
            try:
                hash(values)
            except TypeError as exc:
                raise TypeError(
                    f"rule[{index}] equality values must be hashable"
                ) from exc
            bucket = grouped.setdefault(fields, defaultdict(list))
            bucket[values].append(rule["label"])
        return {
            fields: {values: tuple(labels) for values, labels in index.items()}
            for fields, index in grouped.items()
        }

    def _build_accessor(self, fields: tuple[str, ...]) -> Any:
        if self.record_mode == "attr":
            return attrgetter(*fields)
        return None

    def _value_tuple(
        self, fields: tuple[str, ...], record: Any
    ) -> tuple[Any, ...] | None:
        try:
            if self.record_mode == "dict":
                return tuple(record[field] for field in fields)
            accessor = self._accessors[fields]
            value = accessor(record)
            if len(fields) == 1:
                return (value,)
            return tuple(value)
        except (AttributeError, KeyError, TypeError):
            return None


def build_eq_rule_labeler(
    payload: Any,
    *,
    record_mode: RecordMode = "attr",
    multi_match: bool = True,
) -> EqRuleLabeler:
    """Build an EqRuleLabeler from a list or a ``{"rules": [...]}`` mapping."""

    if isinstance(payload, Mapping):
        rules = payload.get("rules")
    else:
        rules = payload
    if not isinstance(rules, Sequence) or isinstance(rules, (str, bytes, bytearray)):
        raise TypeError(
            "eq_rule_labeler payload must be a rule list or {'rules': list}"
        )
    return EqRuleLabeler(
        rules,
        record_mode=record_mode,
        multi_match=multi_match,
    )


__all__ = ["EqRuleLabeler", "RecordMode", "build_eq_rule_labeler"]
