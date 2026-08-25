"""THROWAWAY probe for SCS-1413. Fails deliberately so pytest prints the captured
output. Delete with this branch."""

from helpers.data_source_test_helper import DataSourceTestHelper


def test_regex_escaping_probe(data_source_test_helper: DataSourceTestHelper):
    ds = data_source_test_helper.data_source_impl
    dialect = ds.sql_dialect
    out = ["", f"dialect: {dialect.__class__.__name__}"]
    out.append(f"escape_string('a{chr(92)}.b') -> {dialect.escape_string('a' + chr(92) + '.b')!r}")

    for label, lit in [("one backslash", r"'a\b'"), ("two backslashes", r"'a\\b'")]:
        try:
            r = ds.execute_query(f"SELECT LENGTH({lit})", log_query=False)
            out.append(f"LENGTH({lit}) = {r.rows[0][0]}  ({label}; 3 => backslash survived)")
        except Exception as e:
            out.append(f"LENGTH({lit}) FAILED {type(e).__name__}: {str(e)[:100]}")

    variants = [
        ("1 backslash  (pre-fix)", r"^1\.5$"),
        ("2 backslashes (post-fix)", r"^1\\.5$"),
        ("4 backslashes", r"^1\\\\.5$"),
        ("char class [.]", "^1[.]5$"),
    ]
    try:
        r = ds.execute_query("SELECT version()", log_query=False)
        out.append(f"version: {str(r.rows[0][0])[:110]}")
    except Exception as e:
        out.append(f"version FAILED {type(e).__name__}")

    for label, pat in variants:
        for op_label, sql in [
            ("~ ", f"SELECT '1.5' ~ '{pat}', '1x5' ~ '{pat}'"),
            ("RL", f"SELECT REGEXP_LIKE('1.5', '{pat}'), REGEXP_LIKE('1x5', '{pat}')"),
        ]:
            try:
                r = ds.execute_query(sql, log_query=False)
                dot, x = r.rows[0]
                verdict = "CORRECT" if (dot and not x) else "wrong"
                out.append(f"{op_label} {label:26} '1.5'={str(dot):>5} '1x5'={str(x):>5}  {verdict}")
            except Exception as e:
                out.append(f"{op_label} {label:26} FAILED {type(e).__name__}: {str(e)[:60]}")

    raise AssertionError("PROBE RESULTS\n" + "\n".join(out))
