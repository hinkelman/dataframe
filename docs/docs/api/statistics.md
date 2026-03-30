# Statistics

All statistical procedures ignore `'na` values by default (`na-rm` defaults to `#t`). Pass `#f` to propagate `'na`.

Booleans are coerced to numbers: `#t` → `1`, `#f` → `0`.

---

## List utilities

### `(count obj lst)`

**returns:** the number of occurrences of `obj` in `lst`.

### `(count-elements lst)`

**returns:** a list of `(element . count)` pairs for every unique element in `lst`.

### `(rle lst)`

**returns:** run-length encoding of `lst` as a list of `(element . count)` pairs.

### `(remove-duplicates lst)`

**returns:** `lst` with duplicate elements removed, preserving first-occurrence order.

### `(rep lst n type)`

**returns:** `lst` repeated `n` times. `type` must be `'times` (repeat the whole list) or `'each` (repeat each element).

### `(transpose lst)`

**returns:** the transposed list-of-lists.

```scheme
> (define x '(a b b c c c d d d d na))

> (count 'c x)
3

> (count-elements x)
((a . 1) (b . 2) (c . 3) (d . 4) (na . 1))

> (rle '(1 1 2 1 1 0 2 2))
((1 . 2) (2 . 1) (1 . 2) (0 . 1) (2 . 2))

> (remove-duplicates x)
(a b c d na)

> (rep '(1 2) 3 'times)
(1 2 1 2 1 2)

> (rep '(1 2) 3 'each)
(1 1 1 2 2 2)

> (transpose '((1 2 3) (4 5 6)))
((1 4) (2 5) (3 6))
```

---

## Descriptive statistics

### `(sum lst [na-rm])`

**returns:** the sum of values in `lst`.

```scheme
> (sum (iota 10))
45
> (sum (cons 'na (iota 10)))   ; na-rm defaults to #t
45
> (sum (cons 'na (iota 10)) #f)
na
> (sum '(#t #f #t #f #t))
3
```

### `(product lst [na-rm])`

**returns:** the product of values in `lst`.

```scheme
> (product (map add1 (iota 10)))
3628800
> (product '(#t #f #t #f #t))
0
```

### `(mean lst [na-rm])`

**returns:** the arithmetic mean of values in `lst`.

```scheme
> (mean '(1 2 3 4 5))
3
> (mean '(-10 0 10 na) #f)
na
> (mean '(#t #f #t na))
2/3
```

### `(weighted-mean lst weights [na-rm])`

**returns:** the weighted arithmetic mean. `na-rm` applies only to `lst`; any `'na` in `weights` yields `'na`.

```scheme
> (weighted-mean '(1 2 3 4 5) '(5 4 3 2 1))
7/3
> (weighted-mean '(1 2 3 4 5) '(5 4 3 2 na))
na
```

### `(variance lst [na-rm])`

**returns:** the sample variance using [Welford's algorithm](https://www.johndcook.com/blog/standard_deviation/).

```scheme
> (inexact (variance '(1 10 100 1000)))
233840.25
> (variance '(0 1 2 3 4 5))
7/2
```

### `(standard-deviation lst [na-rm])`

**returns:** the sample standard deviation.

```scheme
> (standard-deviation '(0 1 2 3 4 5))
1.8708286933869707
```

### `(median lst [type na-rm])`

**returns:** the median of `lst`. Uses `type` 8 by default (see `quantile`).

```scheme
> (median '(1 2 3 4 5 6))
3.5
```

### `(quantile lst p [type na-rm])`

**returns:** the sample quantile at probability `p`. `type` is an integer 1–9 following [Hyndman and Fan (1996)](https://www.jstor.org/stable/2684934); defaults to type 8. R's default is type 7.

```scheme
> (quantile '(1 2 3 4 5 6) 0.5 1)
3
> (quantile '(1 2 3 4 5 6) 0.5 8)
3.5
> (quantile '(1 2 3 4 5 6) 0.025 7)
1.125
```

### `(interquartile-range lst [type na-rm])`

**returns:** the difference between the 0.75 and 0.25 quantiles. Defaults to type 8.

```scheme
> (interquartile-range '(1 2 3 5 5))
3.3333333333333335
> (interquartile-range '(1 2 3 5 5) 1)
3
```

### `(cumulative-sum lst)`

**returns:** a list of running totals. Propagates `'na` once encountered.

```scheme
> (cumulative-sum '(1 2 3 4 5))
(1 3 6 10 15)
> (cumulative-sum '(1 2 3 na 4))
(1 3 6 na na)
```
