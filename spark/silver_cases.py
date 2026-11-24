"""The rules of ``silver.cases`` and ``silver.lab_results`` that need no Spark: the columns and their types, how a published value becomes a typed one, the
columns that are never features, the measured length of an operation with the score cutoffs that come from it, and what each check refuses.

The Spark job ``spark/jobs/silver_cases.py`` reads ``bronze.clinical_cases`` and ``bronze.lab_results`` (every column a string) and ``silver.patient_split``.
Silver does the standard steps only: it enforces types, keeps a missing value missing, deduplicates and checks. It fills nothing, converts no unit and
builds no join that Gold does not need. A value keeps the unit that the publisher gave it, and a time is in seconds from the start of its case.

The test set is sealed: nothing here names a case or a patient. The clinical table holds the outcome columns, so no count over the cases leaves the job: every
failure is a code and a flag (``key_null: failed``), never a count. The operation lengths that set the cutoffs come from
development patients only.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction

from spark.result_file import SealedError
from spark.split_table import COLUMNS as SPLIT_COLUMNS
from spark.split_table import SplitRow, table_problems

BRONZE_CASES = "bronze.clinical_cases"
BRONZE_LAB = "bronze.lab_results"
SPLIT_TABLE = "silver.patient_split"
CASES_TABLE = "silver.cases"
LAB_TABLE = "silver.lab_results"
MEASURE_TABLE = "silver.operation_length_measure"
CUTOFFS_TABLE = "silver.score_cutoffs"
DEVELOPMENT_SETS = ("training", "validation")
PINNED_CASES = 6388
PINNED_LAB_ROWS = 928448
INT_PATTERN = "-?[0-9]+"
NUMBER_PATTERN = "-?[0-9]+(\\.[0-9]+)?"
INT_FORMAT = re.compile(INT_PATTERN, re.ASCII)
NUMBER_FORMAT = re.compile(NUMBER_PATTERN, re.ASCII)
BIGINT_LIMIT = 2**63
TOP_CODED_AGE = ">89"
SECONDS_PER_MINUTE = 60
BIGINT = "bigint"
DOUBLE = "double"
STRING = "string"
PERCENTILES = (5, 10, 25, 50, 75, 90, 95)
CUTOFF_FRACTIONS = (25, 50, 75)
CUTOFF_RULE_VERSION = f"operation_cutoffs:fractions_of_median={','.join(str(percent) for percent in CUTOFF_FRACTIONS)}:whole_minutes:exact_median"
FLAG_COLUMNS = ("emop", "preop_htn", "preop_dm", "death_inhosp")
# The outcome columns: the stay in intensive care and the death. Their values are checked and counted for development patients only; for the held-out
# patients the job checks their form inside the job and reports one flag, never a count.
OUTCOME_COLUMNS = ("icu_days", "death_inhosp")
FLAG = "failed"
OWNERSHIP_PROBLEM = f"ambiguous_case_owner: {FLAG}"
SEX_VALUES = ("F", "M")
ASA_VALUES = (1, 2, 3, 4, 5, 6)

# The 74 columns of the pinned release's clinical_data.csv, in the publisher's order, each with the type of its published form: a whole number, a number
# with a decimal part, or text. `age` is a number except for the publisher's top-coded value ">89", which is kept apart (see ``age_over_89``).
CLINICAL_HEADER = (
    "caseid,subjectid,casestart,caseend,anestart,aneend,opstart,opend,adm,dis,icu_days,death_inhosp,age,sex,height,weight,bmi,asa,emop,department,optype,dx,"
    "opname,approach,position,ane_type,preop_htn,preop_dm,preop_ecg,preop_pft,preop_hb,preop_plt,preop_pt,preop_aptt,preop_na,preop_k,preop_gluc,preop_alb,"
    "preop_ast,preop_alt,preop_bun,preop_cr,preop_ph,preop_hco3,preop_be,preop_pao2,preop_paco2,preop_sao2,cormack,airway,tubesize,dltubesize,lmasize,iv1,"
    "iv2,aline1,aline2,cline1,cline2,intraop_ebl,intraop_uo,intraop_rbc,intraop_ffp,intraop_crystalloid,intraop_colloid,intraop_ppf,intraop_mdz,intraop_ftn,"
    "intraop_rocu,intraop_vecu,intraop_eph,intraop_phe,intraop_epi,intraop_ca"
)
PUBLISHED_CLINICAL_COLUMNS = tuple(CLINICAL_HEADER.split(","))
WHOLE_NUMBER_TEXT = (
    "caseid casestart caseend anestart opstart opend adm dis icu_days death_inhosp asa emop preop_htn preop_dm preop_plt "
    "preop_pt preop_na preop_gluc preop_ast preop_alt preop_bun lmasize intraop_ebl intraop_uo intraop_rbc intraop_ffp "
    "intraop_crystalloid intraop_colloid intraop_ppf intraop_ftn intraop_rocu intraop_vecu intraop_eph intraop_phe "
    "intraop_epi intraop_ca"
)
WHOLE_NUMBER_COLUMNS = frozenset(WHOLE_NUMBER_TEXT.split())
DECIMAL_TEXT = (
    "aneend age height weight bmi preop_hb preop_aptt preop_k preop_alb preop_cr preop_ph preop_hco3 preop_be preop_pao2 preop_paco2 preop_sao2 tubesize "
    "intraop_mdz"
)
DECIMAL_COLUMNS = frozenset(DECIMAL_TEXT.split())
AGE_COLUMN = "age"
AGE_OVER_COLUMN = "age_over_89"
OCCURRENCE_COLUMN = "source_occurrence"
COPIES_COLUMN = "source_copies"
LAB_PUBLISHED_COLUMNS = ("caseid", "dt", "name", "result")
LAB_GROUP = ("caseid", "dt", "name")
NEVER_FEATURE_PREFIXES = ("intraop_",)
NEVER_FEATURE_NAMES = ("caseend", "opend", "aneend", "dis", "icu_days", "death_inhosp")


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: str


def column_type(name: str) -> str:
    if name in WHOLE_NUMBER_COLUMNS:
        return BIGINT
    return DOUBLE if name in DECIMAL_COLUMNS else STRING


def case_columns() -> tuple[Column, ...]:
    """Every published column with its type, and ``age_over_89`` (a boolean) right after ``age``."""
    columns: list[Column] = []
    for name in PUBLISHED_CLINICAL_COLUMNS:
        columns.append(Column(name, column_type(name)))
        if name == AGE_COLUMN:
            columns.append(Column(AGE_OVER_COLUMN, "boolean"))
    return tuple(columns)


CASE_COLUMNS = case_columns()
LAB_COLUMNS = (
    Column("caseid", BIGINT),
    Column("dt", BIGINT),
    Column("name", STRING),
    Column("result", DOUBLE),
    Column(COPIES_COLUMN, "int"),
    Column(OCCURRENCE_COLUMN, "int"),
)
# The fields that are never features: the end times, the stay in intensive care, the death in hospital and every intraoperative total. They stay in Silver,
# marked in the table's property `never_feature_columns`; a fast test holds this list to the feature code's own refusal list, so the two cannot differ.
NEVER_FEATURE_COLUMNS = tuple(column.name for column in CASE_COLUMNS if column.name in NEVER_FEATURE_NAMES or column.name.startswith(NEVER_FEATURE_PREFIXES))
NEVER_FEATURE_PROPERTY = "never_feature_columns"


LAB_BRONZE_COLUMNS = (*((name, STRING) for name in LAB_PUBLISHED_COLUMNS), (OCCURRENCE_COLUMN, "int"))
# The columns and types of the three tables that the job reads, as Bronze and the patient split write them.
SOURCE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    BRONZE_CASES: [(name, STRING) for name in PUBLISHED_CLINICAL_COLUMNS],
    BRONZE_LAB: list(LAB_BRONZE_COLUMNS),
    SPLIT_TABLE: list(SPLIT_COLUMNS),
}


class SilverCasesError(SealedError):
    """A rule was broken or a check failed. ``problems`` holds one message for each cause, each a code and a flag."""


def flag(code: str) -> str:
    """The one form of a refusal: the code and a flag. It never holds a count, a name or a value."""
    return f"{code}: {FLAG}"


def typed_value(kind: str, raw: str | None) -> int | float | str | None:
    """The typed value of a published text, by the one rule that the Spark expressions of ``silver_cases_sql.py`` follow.

    A missing value (no value, or an empty text) stays missing; any other text is a value, a blank one included, and is never trimmed. A whole number is
    an optional minus and ASCII digits within the signed 64 bits. A decimal number is an optional minus, digits, and digits after a point. Nothing else is
    a number (no plus, no exponent, no space, no ``NaN``), and such a value is a ``SilverCasesError`` here and a counted failure in the job. A text is
    returned as it is.
    """
    if raw is None or raw == "":
        return None
    if kind == STRING:
        return raw
    if kind == BIGINT and INT_FORMAT.fullmatch(raw) and -BIGINT_LIMIT <= int(raw) < BIGINT_LIMIT:
        return int(raw)
    if kind == DOUBLE and NUMBER_FORMAT.fullmatch(raw):
        return float(raw)
    raise SilverCasesError(flag("unparsed"))


def age_over_89_value(raw: str | None) -> bool | None:
    """Whether the published age is the top-coded text ``>89``: unknown when the age is missing. The number column ``age`` is missing for that text."""
    if raw is None or raw == "":
        return None
    return raw == TOP_CODED_AGE


def nearest_rank(values: Sequence[int], percent: int) -> int:
    """The value of the sorted ``values`` at rank ``ceil(percent * n / 100)`` (counting from 1)."""
    rank = -(-percent * len(values) // 100)
    return sorted(values)[max(rank, 1) - 1]


@dataclass(frozen=True, slots=True)
class MeasureRow:
    statistic: str
    unit: str
    value: int


@dataclass(frozen=True, slots=True)
class CutoffRow:
    cutoff: str
    anchor: str
    elapsed_seconds: int | None
    basis: str
    rule_version: str
    development_cases: int
    median_length_seconds: int


def check_lengths(lengths: Sequence[int]) -> None:
    if not lengths:
        raise SilverCasesError(flag("no_development_cases"))
    if any(length < 0 for length in lengths):
        raise SilverCasesError(flag("bad_length"))


def measure_rows(lengths: Sequence[int]) -> list[MeasureRow]:
    """The measured distribution of the operation lengths (``opend`` minus ``opstart``, in seconds) of the development cases: their count, the least and
    the greatest length and the percentiles by nearest rank. The order of ``lengths`` does not matter."""
    check_lengths(lengths)
    ordered = sorted(lengths)
    rows = [MeasureRow("cases", "cases", len(ordered)), MeasureRow("min", "seconds", ordered[0])]
    rows += [MeasureRow(f"p{percent}", "seconds", nearest_rank(ordered, percent)) for percent in PERCENTILES]
    return [*rows, MeasureRow("max", "seconds", ordered[-1])]


def median_of(lengths: Sequence[int]) -> Fraction:
    """The median: the middle length of an odd number of lengths, the mean of the two middle lengths of an even number. Exact, never rounded."""
    ordered = sorted(lengths)
    middle = len(ordered) // 2
    return Fraction(ordered[middle]) if len(ordered) % 2 else Fraction(ordered[middle - 1] + ordered[middle], 2)


def cutoff_rows(lengths: Sequence[int]) -> list[CutoffRow]:
    """The score cutoffs: the start (``opstart``), a quarter, a half and three quarters of the median development operation length after the start, each
    rounded down to a whole minute after the exact fraction of the exact median, and the final (``opend``). ``median_length_seconds`` is the median rounded
    down to a second. The times must be strictly increasing, or the median is too short to set them."""
    check_lengths(lengths)
    median = median_of(lengths)
    elapsed = [math.floor(median * percent / (100 * SECONDS_PER_MINUTE)) * SECONDS_PER_MINUTE for percent in CUTOFF_FRACTIONS]
    if [0, *elapsed] != sorted({0, *elapsed}):
        raise SilverCasesError(flag("cutoffs_not_increasing"))
    common = (CUTOFF_RULE_VERSION, len(lengths), math.floor(median))
    rows = [CutoffRow("start", "opstart", 0, "fixed", *common)]
    rows += [
        CutoffRow(f"p{percent}_of_median", "opstart", seconds, f"{percent} percent of the median", *common)
        for percent, seconds in zip(CUTOFF_FRACTIONS, elapsed, strict=True)
    ]
    return [*rows, CutoffRow("final", "opend", None, "fixed", *common)]


def same_rows(first: Sequence[object], second: Sequence[object]) -> bool:
    """Whether two lists hold the same rows, each as many times, whatever their order."""
    return sorted(first, key=repr) == sorted(second, key=repr)


def repeated_keys(keys: Sequence[str]) -> int:
    """How many keys are there more than once."""
    return sum(1 for count in Counter(keys).values() if count > 1)


def changed_count(held: Mapping[str, object], wanted: Mapping[str, object]) -> int:
    return sum(1 for name in held.keys() | wanted.keys() if held.get(name) != wanted.get(name))


def frozen_problems(
    existing_measure: Sequence[MeasureRow] | None, existing_cutoffs: Sequence[CutoffRow] | None, measure: Sequence[MeasureRow], cutoffs: Sequence[CutoffRow]
) -> list[str]:
    """How a rebuild would change the measure and the cutoffs that the branch already holds. ``None`` for both is a first build and has nothing to freeze.

    A held table with no row, with a key twice (an equal copy included) or without its partner is refused first. Then every cutoff row is compared in full
    (time, basis, rule, development count and median) and every measure row in full, in any order.
    """
    if existing_measure is None and existing_cutoffs is None:
        return []
    if existing_measure is None or existing_cutoffs is None:
        return [flag("inherited_incomplete")]
    if not existing_measure or not existing_cutoffs:
        return [flag("inherited_empty")]
    if repeated_keys([row.statistic for row in existing_measure]) + repeated_keys([row.cutoff for row in existing_cutoffs]):
        return [flag("inherited_duplicates")]
    changed_cutoffs = changed_count({row.cutoff: row for row in existing_cutoffs}, {row.cutoff: row for row in cutoffs})
    changed_measure = changed_count({row.statistic: row for row in existing_measure}, {row.statistic: row for row in measure})
    return [flag(code) for code, count in (("changed_cutoff", changed_cutoffs), ("changed_measure", changed_measure)) if count]


def split_rule_problems(rows: Sequence[SplitRow], *, pinned: bool) -> list[str]:
    """What is wrong with the patient split table by itself, by the split job's own checks: a patient twice, a set that the one rule of
    ``scripts/sample/split.py`` does not give, another rule version, mixed source snapshots. The set of a development patient is only trusted after this."""
    return table_problems(rows, pinned=pinned)


def source_schema_problems(found: Mapping[str, Sequence[tuple[str, str]] | None]) -> list[str]:
    """The tables that are missing and the tables whose columns or types are not the expected ones, each as a code and a flag. ``found`` maps a table to its
    columns, or to ``None`` when the table is not there."""
    missing = sum(1 for table in SOURCE_SCHEMAS if found.get(table) is None)
    wrong = sum(1 for table, fields in SOURCE_SCHEMAS.items() if found.get(table) is not None and list(found[table] or ()) != fields)
    return [flag(code) for code, count in (("source_missing", missing), ("source_schema", wrong)) if count]


def input_problems(
    *,
    bronze_case_rows: int,
    case_key_null: int,
    case_key_duplicates: int,
    subjectid_form: int,
    unparsed: Mapping[str, int],
    domain: Mapping[str, int],
    operation_ends_before_it_starts: int,
    cases_without_one_split_row: int,
    split_patients_without_case: int,
    lab_source_empty: int,
    lab_key_null: int,
    lab_unparsed: Mapping[str, int],
    lab_rows_without_case: int,
    sealed_outcome_form_bad: int,
    sealed_lab_form_bad: int,
    lab_pinned_mismatch: int,
    development_cases: int | None,
    pinned: bool,
) -> list[str]:
    """What is wrong with the inputs, found before anything is written. Each cause is one code and a flag. No count, value or identifier is named."""
    problems = [
        flag(code)
        for code, bad in (
            ("empty_cases", bronze_case_rows == 0),
            ("empty_lab", lab_source_empty),
            ("key_null", case_key_null),
            ("key_duplicate", case_key_duplicates),
            ("subjectid_form", subjectid_form),
            ("operation_order", operation_ends_before_it_starts),
            ("split_join", cases_without_one_split_row),
            ("split_patient_without_case", split_patients_without_case),
            ("lab_key_null", lab_key_null),
            ("lab_case_missing", lab_rows_without_case),
            ("unparsed", any(unparsed.values())),
            ("domain", any(domain.values())),
            ("lab_unparsed", any(lab_unparsed.values())),
            ("sealed_outcome_form", sealed_outcome_form_bad),
            ("sealed_lab_form", sealed_lab_form_bad),
            ("no_development_cases", development_cases == 0),
        )
        if bad
    ]
    if pinned and (bronze_case_rows != PINNED_CASES or lab_pinned_mismatch):
        problems.append(flag("pinned_counts"))
    return problems


def output_problems(
    *,
    case_rows: int,
    bronze_case_rows: int,
    case_value_differences: int,
    case_key_duplicates: int,
    lab_rows: int,
    lab_copies: int,
    bronze_lab_rows: int,
    lab_value_differences: int,
    lab_key_duplicates: int,
    lab_occurrence_gaps: int,
    never_feature_marking_wrong: int,
    case_key_differences: int,
    sealed_outcome_differs: int,
    silver_lab_orphans: int,
    sealed_lab_differs: int,
) -> list[str]:
    """What is wrong with the tables that were written, against Bronze: a row count that differs, a changed value, a repeated key or a gap in the occurrence.

    Each cause is one code and a flag. The counts are compared here and never written."""
    return [
        flag(code)
        for code, bad in (
            ("case_rows", case_rows != bronze_case_rows),
            ("lab_rows", lab_copies != bronze_lab_rows or lab_rows > lab_copies),
            ("case_value_differences", case_value_differences),
            ("case_key_duplicate", case_key_duplicates),
            ("case_key_differences", case_key_differences),
            ("lab_value_differences", lab_value_differences),
            ("lab_key_duplicate", lab_key_duplicates),
            ("lab_occurrence_gap", lab_occurrence_gaps),
            ("never_feature_marking", never_feature_marking_wrong),
            ("sealed_outcome_differs", sealed_outcome_differs),
            ("lab_case_missing_in_silver", silver_lab_orphans),
            ("sealed_lab_differs", sealed_lab_differs),
        )
        if bad
    ]
