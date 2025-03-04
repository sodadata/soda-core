# Thresholds

Checks based on a numeric metric value can have thresholds specified like this:

The check metric value must be a specific value:
```
must_be: 1
```

The check metric value must not be a specified value:
```
must_not_be: 1
```

The check metric value must be greater than a specified value
```
must_be_greater_than: 1
```

The check metric value must be greater than or equal to a specified value
```
must_be_greater_than_or_equal: 1
```

The check metric value must be less than a specified value
```
must_be_less_than: 1
```

The check metric value must be less than or equal to a specified value
```
must_be_less_than_or_equal: 1
```

The check metric value must be between 2 boundary values.  The boundary 
values themselves are considered valid values as well.
```
must_be_between: [10, 25]
```

The check metric value must be outside 2 boundary values.  The boundary 
values themselves are considered valid values as well.
```
must_be_not_between: [10, 25]
```

Custom inner range with including and excluding boundaries.  
The check metric value must be greater than 10 and less than or equal to 25:
```
must_be_greater_than: 10
must_be_less_than_or_equal: 25
```

Custom outer range with including and excluding boundaries.  
The check metric value must be less than or equal to 10 or greater than 25:
```
must_be_less_than_or_equal: 10
must_be_greater_than: 25
```
