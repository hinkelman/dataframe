# Dataframe

## Constructors

### `(make-dataframe slist)`

**returns:** a dataframe from a list of series `slist`.

### `(make-df* expr ...)`

**returns:** a dataframe from one or more bare expressions of the form `(name val ...)`. More concise than `make-dataframe`.

```scheme
> (make-df* (a 1 2 3) (b 4 5 6))
#[#{dataframe ...} ...]

> (make-df* ("a" 1 2 3))
; Exception: name(s) not symbol(s)
```

---

## Predicates

### `(dataframe? obj)`

**returns:** `#t` if `obj` is a dataframe, `#f` otherwise.

### `(dataframe-equal? df1 df2 ...)`

**returns:** `#t` if all dataframes are equal, `#f` otherwise. Column order matters.

```scheme
> (dataframe-equal? (make-df* (a 1 2 3))
                    (make-df* (a 1 "2" 3)))
#t

> (dataframe-equal? (make-df* (a 1 2 3) (b 4 5 6))
                    (make-df* (b 4 5 6) (a 1 2 3)))
#f
```

### `(dataframe-contains? df name ...)`

**returns:** `#t` if all column `name`s are found in dataframe `df`, `#f` otherwise.

```scheme
> (define df (make-df* (a 1) (b 2) (c 3) (d 4)))

> (dataframe-contains? df 'a 'c 'd)
#t

> (dataframe-contains? df 'b 'e)
#f
```

---

## Accessors

### `(dataframe-slist df)`

**returns:** the list of series comprising dataframe `df`.

### `(dataframe-names df)`

**returns:** a list of column name symbols.

### `(dataframe-dim df)`

**returns:** a pair `(rows . cols)`.

```scheme
> (dataframe-names (make-df* (a 1) (b 2) (c 3) (d 4)))
(a b c d)

> (dataframe-dim (make-df* (a 1 2 3) (b 4 5 6)))
(3 . 2)
```

### `(dataframe-series df name)`

**returns:** the series for column `name` from dataframe `df`.

### `(dataframe-values df name)`

**returns:** the list of values for column `name` from dataframe `df`.

### `($ df name)`

Shorthand for `dataframe-values`. Inspired by R's `df$name` syntax.

```scheme
> (define df (make-df* (a 100 200 300) (b 4 5 6) (c 700 800 900)))

> (dataframe-values df 'b)
(4 5 6)

> ($ df 'b)
(4 5 6)

> (map (lambda (name) ($ df name)) '(c a))
((700 800 900) (100 200 300))
```

---

## Row Operations

### `(dataframe-head df n)`

**returns:** a dataframe with the first `n` rows of `df`.

### `(dataframe-tail df n)`

**returns:** a dataframe beginning at the `n`th row (zero-based) of `df`. Follows `list-tail` semantics, not R's `tail`.

```scheme
> (define df (make-df* (a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999)))

> (dataframe-display (dataframe-head df 3))

 dim: 3 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.

> (dataframe-display (dataframe-tail df 2))

 dim: 4 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      3.      6.      9.
      1.      4.   -999.
      2.      5.   -999.
      3.      6.   -999.
```

### `(dataframe-ref df indices [name ...])`

**returns:** a dataframe with only the rows at zero-based `indices`. Optionally restrict to specific column `name`s.

```scheme
> (define df (make-df* (a 100 200 300) (b 4 5 6) (c 700 800 900)))

> (dataframe-display (dataframe-ref df '(0 2)))

 dim: 2 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
    100.      4.    700.
    300.      6.    900.

> (dataframe-display (dataframe-ref df '(0 2) 'a 'c))

 dim: 2 rows x 2 cols
       a       c
   <num>   <num>
    100.    700.
    300.    900.
```
