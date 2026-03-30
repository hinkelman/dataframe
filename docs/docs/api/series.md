# Series

A series is a named, typed list. It is the building block of a dataframe.

## Constructors

### `(make-series name lst)`

**returns:** a series record type from `name` (a symbol) and `lst`, with fields: name, lst, length, and type.

### `(make-series* expr)`

**returns:** a series record type from a bare expression `(name val ...)`, inferring the name and values from the expression. Equivalent to `make-series` but more concise.

```scheme
> (make-series 'a '(1 2 3))
#[#{series ...} a (1 2 3) (1 2 3) num 3]

> (make-series* (a 1 2 3))
#[#{series ...} a (1 2 3) (1 2 3) num 3]

> (make-series 'a '(a b c))
#[#{series ...} a (a b c) (a b c) sym 3]

> (make-series* (a 'a 'b 'c))
#[#{series ...} a (a b c) (a b c) sym 3]
```

---

## Predicates

### `(series? obj)`

**returns:** `#t` if `obj` is a series, `#f` otherwise.

### `(series-equal? series1 series2 ...)`

**returns:** `#t` if all series are equal, `#f` otherwise.

```scheme
> (series-equal? (make-series* (a 1 2 3))
                 (make-series* (a 1 "2" 3)))
#t
```

---

## Accessors

### `(series-name series)`

**returns:** the name of `series` as a symbol.

### `(series-lst series)`

**returns:** the list of values in `series`.

### `(series-length series)`

**returns:** the number of elements in `series`.

### `(series-type series)`

**returns:** the inferred type of `series` as a symbol (`bool`, `chr`, `str`, `sym`, `num`, or `other`).

```scheme
> (define s (make-series 'a (iota 10)))

> (series-name s)
a

> (series-lst s)
(0 1 2 3 4 5 6 7 8 9)

> (series-length s)
10

> (series-type s)
num

> (series-type (make-series* (a 1 'b "c")))
str
```
