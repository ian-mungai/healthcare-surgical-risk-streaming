"""The Spark SQL text of the reconciliation: which keys each side holds, how a Silver row is expanded to the Bronze keys that it accounts for, which rows count
as development and what each query returns.

No Spark runs here, so these tests hold the text to the rules: a comparison that leaves out one direction, a null-safe key that is not null-safe, a held-out
query that returns a count, a development filter that lets the test set in, a ledger that is not expanded or a value written into a query would each change the
reconciliation without a failure. The queries run in the acceptance run.
"""

from __future__ import annotations

import re
import unittest

from features.signal_validity import INVALID_CAUSES
from spark.reconcile.checks import JOINS, RECONCILIATIONS, JoinFacts, KeyFacts, ReconcileError
from spark.reconcile.sql import (
    BRONZE_CASES,
    BRONZE_LAB,
    BRONZE_NUMERIC,
    BRONZE_TRACKS,
    BRONZE_WAVEFORM,
    SILVER_CASES,
    SILVER_LAB,
    SILVER_NUMERIC,
    SILVER_WAVEFORM,
    SPLIT,
    decode_join_facts,
    decode_key_facts,
    held_out_query,
    join_held_out_query,
    join_query,
    key_facts_query,
)
from spark.silver_cases_sql import reference

BY_NAME = {reconciliation.name: reconciliation for reconciliation in RECONCILIATIONS}
SILVER_TABLES = {"numeric_observations": SILVER_NUMERIC, "waveform_seconds": SILVER_WAVEFORM, "cases": SILVER_CASES, "lab_results": SILVER_LAB}
LITERAL = re.compile(r"'([^']*)'")


def select_items(query: str) -> list[str]:
    """The expressions of the last top-level SELECT of a query, split at the commas that are not inside parentheses."""
    depth, last, ends = 0, 0, len(query)
    for index, character in enumerate(query):
        depth += character == "("
        depth -= character == ")"
        if depth == 0 and query.startswith("SELECT ", index) and (index == 0 or query[index - 1] == " "):
            last, ends = index + len("SELECT "), len(query)
        if depth == 0 and last and index >= last and query.startswith(" FROM ", index) and ends == len(query):
            ends = index
    items, depth, current = [], 0, ""
    for character in query[last:ends]:
        depth += character == "("
        depth -= character == ")"
        if character == "," and depth == 0:
            items.append(current.strip())
            current = ""
        else:
            current += character
    return [*items, current.strip()]


def aliases(query: str) -> list[str]:
    return [item.rsplit(" AS ", 1)[1].strip() for item in select_items(query)]


class Development(unittest.TestCase):
    def test_every_query_returns_the_columns_that_the_decoder_reads(self) -> None:
        for name, reconciliation in BY_NAME.items():
            with self.subTest(name=name):
                wanted = ["bronze_keys", "bronze_repeats", "silver_repeats", "matched", "unaccounted", "claimed_twice", "silver_unbacked"]
                wanted += [f"disp__{disposition}" for disposition in (*reconciliation.dispositions, "unrecorded")]
                self.assertEqual(wanted, aliases(key_facts_query(name)))

    def test_the_development_query_keeps_the_development_sets_only(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                query = key_facts_query(name)
                self.assertIn("LEFT SEMI JOIN dev", query)
                self.assertNotIn("LEFT ANTI JOIN dev", query)
                self.assertIn("IN ('training', 'validation')", query)
                self.assertNotIn("'test'", query)

    def test_the_held_out_query_keeps_every_other_row_and_returns_one_flag_and_no_count(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                query = held_out_query(name)
                self.assertIn("LEFT ANTI JOIN dev", query)
                self.assertNotIn("LEFT SEMI JOIN dev", query)
                self.assertEqual(["n"], aliases(query))
                self.assertRegex(select_items(query)[0], r"^CASE WHEN .* > 0 THEN 1 ELSE 0 END AS n$")

    def test_the_held_out_flag_covers_every_defect_that_the_development_counts_cover(self) -> None:
        for name in BY_NAME:
            query = held_out_query(name)
            for part in ("unaccounted", "claimed", "unbacked", "repeats", "unrecorded"):
                with self.subTest(name=name, part=part):
                    self.assertIn(part, query)

    def test_the_held_out_flag_adds_up_every_defect_of_the_development_counts(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                flag = select_items(held_out_query(name))[0]
                for term in (
                    "bf.`bronze_repeats`",
                    "sf.`silver_repeats`",
                    "kf.`unaccounted`",
                    "kf.`claimed_twice`",
                    "kf.`silver_unbacked`",
                    "kf.`disp__unrecorded`",
                ):
                    self.assertIn(term, flag)

    def test_a_key_is_compared_in_both_directions_by_one_full_outer_join_and_every_key_part_is_joined(self) -> None:
        keys = {
            "numeric_observations": ("case_id", "track", "case_time_us", "occurrence"),
            "waveform_seconds": ("case_id", "track", "case_second"),
            "cases": ("caseid",),
            "lab_results": ("caseid", "dt", "name", "result", "copy"),
        }
        for name, columns in keys.items():
            with self.subTest(name=name):
                query = key_facts_query(name)
                self.assertEqual(1, query.count("FULL OUTER JOIN"))
                on = query.split("FULL OUTER JOIN", 1)[1].split(" ON ", 1)[1].split(")", 1)[0]
                for column in columns:
                    self.assertRegex(on, rf"bk\.`{column}` (=|<=>) ck\.`{column}`")

    def test_only_the_missing_result_of_a_laboratory_row_is_a_null_safe_key_part(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                on = key_facts_query(name).split("FULL OUTER JOIN", 1)[1].split(" ON ", 1)[1].split(")", 1)[0]
                self.assertEqual(1 if name == "lab_results" else 0, on.count("<=>"))
                if name == "lab_results":
                    self.assertIn("bk.`result` <=> ck.`result`", on)

    def test_the_two_signal_tables_name_their_own_tables_and_the_ledger_is_expanded(self) -> None:
        numeric, waveform = key_facts_query("numeric_observations"), key_facts_query("waveform_seconds")
        self.assertIn(BRONZE_NUMERIC, numeric)
        self.assertIn(SILVER_NUMERIC, numeric)
        self.assertIn("explode(`duplicate_source_occurrences`)", numeric)
        self.assertIn("'duplicate'", numeric)
        self.assertIn(BRONZE_WAVEFORM, waveform)
        self.assertIn(SILVER_WAVEFORM, waveform)
        self.assertNotIn("explode", waveform)

    def test_the_verdict_of_a_signal_row_is_one_of_the_causes_or_valid_and_anything_else_is_unrecorded(self) -> None:
        for name in ("numeric_observations", "waveform_seconds"):
            with self.subTest(name=name):
                query = key_facts_query(name)
                self.assertIn(f"`invalid_cause` IN ({', '.join(repr(cause) for cause in INVALID_CAUSES)})", query)
                self.assertIn("THEN 'valid'", query)
                self.assertIn("ELSE 'unrecorded' END", query)
                self.assertIn("WHEN `valid` IS NULL THEN 'unrecorded'", query)

    def test_a_laboratory_group_is_expanded_to_its_copies_and_ranked_by_the_job_rule(self) -> None:
        query = key_facts_query("lab_results")
        self.assertIn(BRONZE_LAB, query)
        self.assertIn(SILVER_LAB, query)
        self.assertIn("explode(sequence(1, GREATEST(`source_copies`, 1)))", query)
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY `caseid`, `dt`, `name`, `result` ORDER BY `source_occurrence`)", query)
        self.assertIn("WHEN `source_copies` IS NULL OR `source_copies` < 1 THEN 'unrecorded'", query)
        self.assertIn("'collapsed_copy'", query)

    def test_a_bronze_laboratory_key_repeated_is_counted_on_bronzes_own_key(self) -> None:
        query = key_facts_query("lab_results")
        self.assertIn("GROUP BY `caseid`, `dt`, `name`, `source_occurrence`", query)

    def test_a_case_number_is_typed_by_the_guarded_rule_so_an_untyped_bronze_key_is_unaccounted_and_never_dropped(self) -> None:
        query = key_facts_query("cases")
        self.assertIn(BRONZE_CASES, query)
        self.assertIn(SILVER_CASES, query)
        self.assertIn("RLIKE", query)
        self.assertIn("TRY_CAST", query)

    def test_the_development_cases_are_the_cases_of_the_development_sets_through_bronze_and_the_split(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                query = key_facts_query(name)
                self.assertIn(BRONZE_CASES, query)
                self.assertIn(SPLIT, query)
                self.assertIn("c.`subjectid` = s.`subjectid`", query)

    def test_no_value_of_the_data_is_in_a_query_only_names_sets_dispositions_and_the_typing_patterns(self) -> None:
        allowed = {"training", "validation", "numeric", "waveform", "valid", "duplicate", "kept", "collapsed_copy", "unrecorded", *INVALID_CAUSES, ""}
        queries = (
            [key_facts_query(name) for name in BY_NAME]
            + [held_out_query(name) for name in BY_NAME]
            + [join_query(join.name) for join in JOINS]
            + [join_held_out_query(join.name) for join in JOINS]
        )
        for query in queries:
            for literal in LITERAL.findall(query):
                with self.subTest(literal=literal):
                    self.assertTrue(literal in allowed or literal.startswith("^"), literal)
            self.assertNotRegex(query, r"\b[0-9]{3,}\b")

    def test_an_unknown_table_is_refused(self) -> None:
        for build in (key_facts_query, held_out_query, join_query, join_held_out_query):
            with self.subTest(build=build.__name__), self.assertRaises(ReconcileError):
                build("not_a_table")


class Counters(unittest.TestCase):
    """Each counter of the comparison is held to its whole condition, so that a counter that is turned off, widened or swapped changes a test."""

    CONDITIONS = {
        "matched": "`br` IS NOT NULL AND `sc` = 1",
        "unaccounted": "`br` IS NOT NULL AND `sc` IS NULL",
        "claimed_twice": "`br` IS NOT NULL AND `sc` > 1",
        "silver_unbacked": "`br` IS NULL AND `sc` IS NOT NULL",
    }

    def test_each_counter_counts_its_own_condition_and_nothing_else(self) -> None:
        for name in BY_NAME:
            query = key_facts_query(name)
            for alias, condition in self.CONDITIONS.items():
                with self.subTest(name=name, alias=alias):
                    self.assertIn(f"COALESCE(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END), 0) AS `{alias}`", query)

    def test_the_repeats_of_each_side_count_the_keys_that_two_rows_have(self) -> None:
        for name in BY_NAME:
            query = key_facts_query(name)
            for column, source in (("bronze_repeats", "bos"), ("silver_repeats", "sks")):
                with self.subTest(name=name, column=column):
                    self.assertIn(f"COALESCE(SUM(CASE WHEN `n` > 1 THEN 1 ELSE 0 END), 0) AS `{column}` FROM {source}", query)

    def test_each_disposition_is_counted_among_the_matched_keys_by_its_own_name(self) -> None:
        for name, reconciliation in BY_NAME.items():
            for disposition in (*reconciliation.dispositions, "unrecorded"):
                with self.subTest(name=name, disposition=disposition):
                    condition = f"`br` IS NOT NULL AND `sc` = 1 AND `disposition` = '{disposition}'"
                    self.assertIn(f"COALESCE(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END), 0) AS `disp__{disposition}`", key_facts_query(name))

    def test_the_verdict_of_a_signal_row_is_exactly_the_rule(self) -> None:
        causes = ", ".join(repr(cause) for cause in INVALID_CAUSES)
        rule = (
            "CASE WHEN `valid` IS NULL THEN 'unrecorded' WHEN `valid` AND `invalid_cause` IS NULL THEN 'valid' "
            f"WHEN NOT `valid` AND `invalid_cause` IN ({causes}) THEN `invalid_cause` ELSE 'unrecorded' END AS `disposition`"
        )
        for name in ("numeric_observations", "waveform_seconds"):
            with self.subTest(name=name):
                self.assertIn(rule, key_facts_query(name))

    def test_the_declared_key_of_each_silver_table_is_counted_twice_by_the_key_the_builder_types(self) -> None:
        keys = {
            "numeric_observations": "`case_id`, `track`, `case_time_us`",
            "waveform_seconds": "`case_id`, `track`, `case_second`",
            "cases": "`caseid`",
            "lab_results": "`caseid`, `dt`, `name`, `source_occurrence`",
        }
        for name, key in keys.items():
            with self.subTest(name=name):
                query = key_facts_query(name)
                self.assertIn("sk AS (" + " ".join(("SELECT", key + ",", "COUNT(*) AS `n`", "FROM silver_rows", "GROUP BY", key)) + ")", query)
                self.assertIn(f"{SILVER_TABLES[name]}", query)

    def test_a_laboratory_key_that_bronze_declares_is_its_text_so_a_time_written_two_ways_is_not_a_repeat(self) -> None:
        query = key_facts_query("lab_results")
        own = "`raw_caseid`, `raw_dt`, `raw_name`, `source_occurrence`, `caseid`"
        self.assertIn("bo AS (" + " ".join(("SELECT", own + ",", "COUNT(*) AS `n`", "FROM bronze_rows", "GROUP BY", own)) + ")", query)
        for column in ("caseid", "dt", "name"):
            self.assertIn(f"{reference(column)} AS `raw_{column}`", query)

    def test_the_other_bronze_keys_are_counted_by_the_typed_key_so_two_spellings_of_a_case_are_a_repeat(self) -> None:
        query = key_facts_query("cases")
        self.assertIn("bo AS (SELECT `caseid`, COUNT(*) AS `n` FROM bronze_rows GROUP BY `caseid`)", query)


class Joins(unittest.TestCase):
    def test_every_join_query_returns_the_rows_the_unmatched_and_the_fanned(self) -> None:
        for join in JOINS:
            with self.subTest(join=join.name):
                self.assertEqual(["row_count", "unmatched", "fanned"], aliases(join_query(join.name)))

    def test_each_signal_joins_the_tracks_of_its_own_kind_by_case_and_track(self) -> None:
        for name, silver, kind in (("numeric_track", SILVER_NUMERIC, "numeric"), ("waveform_track", SILVER_WAVEFORM, "waveform")):
            with self.subTest(name=name):
                query = join_query(name)
                self.assertIn(silver, query)
                self.assertIn(f"FROM {BRONZE_TRACKS} WHERE `kind` = '{kind}' GROUP BY `case_id`, `track`", query)
                self.assertIn("x LEFT JOIN (", query)
                self.assertTrue(query.endswith(") p ON x.`case_id` = p.`case_id` AND x.`track` = p.`track`"), query)

    def test_a_laboratory_row_is_joined_to_the_silver_cases_and_a_case_to_the_split(self) -> None:
        lab, case = join_query("lab_case"), join_query("case_split")
        self.assertIn(f"FROM {SILVER_CASES} GROUP BY `caseid`", lab)
        self.assertTrue(lab.endswith(") p ON x.`caseid` = p.`caseid`"), lab)
        self.assertIn(SILVER_LAB, lab)
        self.assertIn(f"FROM {SPLIT} GROUP BY `subjectid`", case)
        self.assertTrue(case.endswith(") p ON x.`subjectid` = p.`subjectid`"), case)
        self.assertIn(SILVER_CASES, case)

    def test_the_unmatched_and_the_fanned_are_told_apart_by_the_count_of_partners(self) -> None:
        for join in JOINS:
            with self.subTest(join=join.name):
                query = join_query(join.name)
                self.assertIn("COALESCE(SUM(CASE WHEN p.`n` IS NULL THEN 1 ELSE 0 END), 0) AS unmatched", query)
                self.assertIn("COALESCE(SUM(CASE WHEN p.`n` > 1 THEN 1 ELSE 0 END), 0) AS fanned", query)

    def test_the_rows_of_a_join_are_the_development_rows_only(self) -> None:
        for join in JOINS:
            with self.subTest(join=join.name):
                query = join_query(join.name)
                self.assertIn("LEFT SEMI JOIN dev", query)
                self.assertNotIn("LEFT ANTI JOIN dev", query)
                self.assertTrue(query.startswith("WITH dev AS ("), query)

    def test_a_held_out_join_failure_is_one_flag_and_never_a_count(self) -> None:
        for join in JOINS:
            with self.subTest(join=join.name):
                query = join_held_out_query(join.name)
                self.assertIn("LEFT ANTI JOIN dev", query)
                self.assertNotIn("LEFT SEMI JOIN dev", query)
                self.assertEqual(["n"], aliases(query))
                self.assertRegex(select_items(query)[0], r"^CASE WHEN .* > 0 THEN 1 ELSE 0 END AS n$")
                self.assertTrue(query.endswith("SELECT CASE WHEN unmatched + fanned > 0 THEN 1 ELSE 0 END AS n FROM jc"), query)


class Decoding(unittest.TestCase):
    ROW = {
        "bronze_keys": 10,
        "bronze_repeats": 0,
        "silver_repeats": 0,
        "matched": 10,
        "unaccounted": 0,
        "claimed_twice": 0,
        "silver_unbacked": 0,
        "disp__kept": 10,
        "disp__unrecorded": 0,
    }

    def test_a_row_is_decoded_into_the_counts_and_the_dispositions(self) -> None:
        facts = decode_key_facts(self.ROW)
        self.assertEqual(KeyFacts(10, 0, 10, 0, 0, 0, {"kept": 10, "unrecorded": 0}, 0), facts)
        self.assertEqual(3, decode_key_facts({**self.ROW, "silver_repeats": 3}).silver_repeats)
        self.assertEqual(2, decode_key_facts({**self.ROW, "bronze_repeats": 2}).bronze_repeats)

    def test_a_count_that_is_missing_negative_or_not_a_whole_number_is_refused(self) -> None:
        for bad in (None, -1, 1.5, "3", True):
            with self.subTest(bad=bad), self.assertRaises(ReconcileError) as caught:
                decode_key_facts({**self.ROW, "unaccounted": bad})
            self.assertEqual(["bad_fact: a count is missing or is not a count"], caught.exception.problems)
        with self.assertRaises(ReconcileError):
            decode_key_facts({name: value for name, value in self.ROW.items() if name != "matched"})

    def test_a_join_row_is_decoded(self) -> None:
        self.assertEqual(JoinFacts(5, 1, 2), decode_join_facts({"row_count": 5, "unmatched": 1, "fanned": 2}))
        with self.assertRaises(ReconcileError):
            decode_join_facts({"row_count": 5, "unmatched": None, "fanned": 2})


if __name__ == "__main__":
    unittest.main()
