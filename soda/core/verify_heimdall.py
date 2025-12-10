from soda.execution.query.query import HeimdallCursor

mock_result = {
    'columns': [{'name': 'col1', 'type': 'STRING'}, {'name': 'col2', 'type': 'INTEGER'}],
    'data': [['val1', 1], ['val2', 2]]
}

cursor = HeimdallCursor(mock_result)

# Test description
print(f"Description: {cursor.description}")
assert len(cursor.description) == 2
assert cursor.description[0][0] == 'col1'

# Test fetchone
row1 = cursor.fetchone()
print(f"Row 1: {row1}")
assert row1 == ('val1', 1)

# Test fetchall (remaining)
rows = cursor.fetchall()
print(f"Rows: {rows}")
assert len(rows) == 1
assert rows[0] == ('val2', 2)

import sys
sys.path.append('/Users/josephparadis/Documents/GitHub/soda-core/soda/core')
print("Verification successful!")
