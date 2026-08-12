"""Fast rule matching helpers for equality and contains predicates."""

from __future__ import annotations

from operator import attrgetter
from typing import Any, Iterable, Literal, Mapping, Sequence


RecordMode = Literal["attr", "dict"]


class EqRuleLabeler:
    """Compiled rule matcher for labeling records.

    Equality rules are compiled once into hash indexes grouped by their field
    set. Mixed ``when`` + ``contains`` rules first use the equality hash index,
    then verify the contains predicates. If all rules are pure ``contains``,
    the matcher switches to a contains-only engine that reads each configured
    field once per record and checks the field's unique keywords in one pass.
    """

    def __init__(
        self,
        rules: Sequence[Mapping[str, Any]],
        *,
        record_mode: RecordMode = "dict",
        multi_match: bool = True,
    ) -> None:
        if record_mode not in ("attr", "dict"):
            raise ValueError("record_mode must be 'attr' or 'dict'")
        self.record_mode = record_mode
        self.multi_match = bool(multi_match)
        self._indexes, self._contains_rules = self._compile(rules)
        self._engine = "contains" if not self._indexes and self._contains_rules else "eq"
        self._accessors = {
            fields: self._build_accessor(fields) for fields in self._indexes
        }
        contains_fields = {
            field
            for index in self._indexes.values()
            for entries in index.values()
            for contains_specs, _label in entries
            for field, _needles in contains_specs
        }
        contains_fields.update(
            field
            for contains_specs, _label in self._contains_rules
            for field, _needles in contains_specs
        )
        self._contains_accessors = {
            field: self._build_accessor((field,))
            for field in contains_fields
        }
        self._contains_only_needles = self._build_contains_only_needles()

    @property
    def field_sets(self) -> tuple[tuple[str, ...], ...]:
        """Field combinations that will be checked at runtime."""

        return tuple(self._indexes)

    @property
    def rule_count(self) -> int:
        """Number of configured rules after compilation."""

        equality_count = sum(
            len(labels) for index in self._indexes.values() for labels in index.values()
        )
        return equality_count + len(self._contains_rules)

    @property
    def engine(self) -> str:
        """Runtime engine selected for the compiled rules."""

        return self._engine

    def match(self, record: Any) -> tuple[Any, ...]:
        """Return labels matching one record."""

        if self._engine == "contains":
            return self._match_contains_only(record)

        labels: list[Any] = []
        for fields, index in self._indexes.items():
            key = self._value_tuple(fields, record)
            if key is None:
                continue
            entries = index.get(key)
            if not entries:
                continue
            for contains_specs, label in entries:
                if contains_specs and not self._all_contains_match(
                    contains_specs, record
                ):
                    continue
                if not self.multi_match:
                    return (label,)
                labels.append(label)
        for contains_specs, label in self._contains_rules:
            if self._all_contains_match(contains_specs, record):
                if not self.multi_match:
                    return (label,)
                labels.append(label)
        return tuple(labels)

    def match_many(self, records: Iterable[Any]) -> list[tuple[Any, ...]]:
        """Return labels for every record without mutating/copying records."""

        return [self.match(record) for record in records]

    @staticmethod
    def _compile(
        rules: Sequence[Mapping[str, Any]],
    ) -> tuple[
        dict[
            tuple[str, ...],
            dict[
                tuple[Any, ...],
                tuple[tuple[tuple[tuple[str, tuple[str, ...]], ...], Any], ...],
            ],
        ],
        tuple[tuple[tuple[tuple[str, tuple[str, ...]], ...], Any], ...],
    ]:
        grouped: dict[
            tuple[str, ...],
            dict[
                tuple[Any, ...],
                list[tuple[tuple[tuple[str, tuple[str, ...]], ...], Any]],
            ],
        ] = {}
        contains_rules: list[
            tuple[tuple[tuple[str, tuple[str, ...]], ...], Any]
        ] = []
        for index, rule in enumerate(rules):
            if not isinstance(rule, Mapping):
                raise TypeError(f"rule[{index}] must be a mapping")
            when = rule.get("when")
            contains = rule.get("contains")
            if when is None and contains is None:
                raise ValueError(
                    f"rule[{index}] requires non-empty 'when' or 'contains' mapping"
                )
            if "label" not in rule:
                raise ValueError(f"rule[{index}] requires 'label'")
            contains_specs = EqRuleLabeler._contains_specs(index, contains)
            if when is not None:
                if not isinstance(when, Mapping) or not when:
                    raise ValueError(
                        f"rule[{index}] 'when' must be a non-empty mapping"
                    )
                pairs = sorted((str(field), value) for field, value in when.items())
                fields = tuple(field for field, _value in pairs)
                values = tuple(value for _field, value in pairs)
                try:
                    hash(values)
                except TypeError as exc:
                    raise TypeError(
                        f"rule[{index}] equality values must be hashable"
                    ) from exc
                bucket = grouped.setdefault(fields, {})
                labels = list(bucket.get(values, ()))
                labels.append((contains_specs, rule["label"]))
                bucket[values] = labels
            elif contains_specs:
                contains_rules.append((contains_specs, rule["label"]))
        return (
            {
                fields: {values: tuple(labels) for values, labels in index.items()}
                for fields, index in grouped.items()
            },
            tuple(contains_rules),
        )

    @staticmethod
    def _contains_specs(
        index: int,
        contains: Any,
    ) -> tuple[tuple[str, tuple[str, ...]], ...]:
        if contains is None:
            return ()
        if not isinstance(contains, Mapping) or not contains:
            raise ValueError(f"rule[{index}] 'contains' must be a non-empty mapping")
        return tuple(
            (str(field), EqRuleLabeler._normalize_needles(raw_needles))
            for field, raw_needles in sorted(contains.items())
        )

    @staticmethod
    def _normalize_needles(raw_needles: Any) -> tuple[str, ...]:
        if isinstance(raw_needles, str):
            needles = (raw_needles,)
        elif isinstance(raw_needles, Sequence) and not isinstance(
            raw_needles, (bytes, bytearray)
        ):
            needles = tuple(str(item) for item in raw_needles)
        else:
            needles = (str(raw_needles),)
        if not needles or any(needle == "" for needle in needles):
            raise ValueError("contains values must not be empty")
        return needles

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

    def _field_value(self, field: str, record: Any) -> Any:
        if self.record_mode == "dict":
            return record[field]
        accessor = self._contains_accessors[field]
        return accessor(record)

    def _build_contains_only_needles(self) -> dict[str, tuple[str, ...]]:
        if self._engine != "contains":
            return {}
        grouped: dict[str, list[str]] = {}
        seen: dict[str, set[str]] = {}
        for contains_specs, _label in self._contains_rules:
            for field, needles in contains_specs:
                field_needles = grouped.setdefault(field, [])
                field_seen = seen.setdefault(field, set())
                for needle in needles:
                    if needle in field_seen:
                        continue
                    field_seen.add(needle)
                    field_needles.append(needle)
        return {field: tuple(needles) for field, needles in grouped.items()}

    def _contains_field_matches(
        self,
        record: Any,
    ) -> dict[str, frozenset[str]]:
        matches: dict[str, frozenset[str]] = {}
        for field, needles in self._contains_only_needles.items():
            try:
                value = self._field_value(field, record)
            except (AttributeError, KeyError, TypeError):
                matches[field] = frozenset()
                continue
            if value is None:
                matches[field] = frozenset()
                continue
            text = value if isinstance(value, str) else str(value)
            matches[field] = frozenset(needle for needle in needles if needle in text)
        return matches

    def _contains_specs_matched(
        self,
        contains_specs: tuple[tuple[str, tuple[str, ...]], ...],
        field_matches: Mapping[str, frozenset[str]],
    ) -> bool:
        return all(
            bool(field_matches.get(field, frozenset()).intersection(needles))
            for field, needles in contains_specs
        )

    def _match_contains_only(self, record: Any) -> tuple[Any, ...]:
        field_matches = self._contains_field_matches(record)
        labels: list[Any] = []
        for contains_specs, label in self._contains_rules:
            if not self._contains_specs_matched(contains_specs, field_matches):
                continue
            if not self.multi_match:
                return (label,)
            labels.append(label)
        return tuple(labels)

    def _contains_match(
        self,
        field: str,
        needles: tuple[str, ...],
        record: Any,
    ) -> bool:
        try:
            value = self._field_value(field, record)
        except (AttributeError, KeyError, TypeError):
            return False
        if value is None:
            return False
        text = value if isinstance(value, str) else str(value)
        return any(needle in text for needle in needles)

    def _all_contains_match(
        self,
        contains_specs: tuple[tuple[str, tuple[str, ...]], ...],
        record: Any,
    ) -> bool:
        return all(
            self._contains_match(field, needles, record)
            for field, needles in contains_specs
        )


def build_eq_rule_labeler(
    payload: Any,
    *,
    record_mode: RecordMode = "dict",
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
