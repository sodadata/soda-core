# A limitation to keep in mind

Regex is not supported in its entirety in T-SQL. In the datasource super class, e-mails are validated with the following expression:

```
^[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%+]+@[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%]+\.[A-Za-z]{2,4}$
```

A valid RegEx expression for e-mails in T-SQL looks something like this:

```
%_@__%.__%
```

This expression is very broad and can match many strings that are not actually email addresses. You can however improve this validation with extended string manipulation or UDFs.
