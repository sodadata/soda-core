# Thresholds

Checks based on a numeric metric value can have thresholds specified like this:

The check metric value must be a specific value:
```
threshold:
  must_be: 1
```

The check metric value must not be a specified value:
```
threshold:
  must_not_be: 1
```

The check metric value must be greater than a specified value
```
threshold:
  must_be_greater_than: 1
```

The check metric value must be greater than or equal to a specified value
```
threshold:
  must_be_greater_than_or_equal: 1
```

The check metric value must be less than a specified value
```
threshold:
  must_be_less_than: 1
```

The check metric value must be less than or equal to a specified value
```
threshold:
  must_be_less_than_or_equal: 1
```

The check metric value must be between 2 boundary values.  The boundary 
values themselves are considered valid values as well: 

`0 <= value <= 10`
```
threshold:
  must_be_between:
    greater_than_or_equal: 0
    less_than_or_equal: 10
```

`0 < value <= 10`
```
threshold:
  must_be_between:
    greater_than: 0
    less_than_or_equal: 10
```

`0 <= value < 10`
```
threshold:
  must_be_between:
    greater_than_or_equal: 0
    less_than: 10
```

`0 < value < 10`
```
threshold:
  must_be_between:
    greater_than: 0
    less_than: 10
```

The check metric value must be outside 2 boundary values.  The boundary 
values themselves are considered valid values as well. 
`value <= 0 OR value >= 10`
```
threshold:
  must_be_not_between:
    less_than_or_equal: 0
    greater_than_or_equal: 10
```

`value < 0 OR value >= 10`
```
threshold:
  must_be_not_between:
    less_than: 0
    greater_than_or_equal: 10
```

`value <= 0 OR value > 10`
```
threshold:
  must_be_not_between:
    less_than_or_equal: 0
    greater_than: 10
```

`value < 0 OR value > 10`
```
threshold:
  must_be_not_between:
    less_than: 0
    greater_than: 10
```
