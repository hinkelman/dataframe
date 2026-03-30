# Missing Values

Missing values are represented by the symbol `'na`. Only the exact symbol `'na` is treated as missing — `"na"` (string), `'NA`, and other variants are not.

## `(na? obj)`

**returns:** `#t` if `obj` is `'na`, `#f` otherwise.

## `(any-na? lst)`

**returns:** `#t` if any element of `lst` is `'na`, `#f` otherwise.

```scheme
> (na? 'na)
#t
> (na? "na")
#f
> (na? 'NA)
#f
> (any-na? (iota 10))
#f
> (any-na? (cons 'na (iota 10)))
#t
> (any-na? (cons "na" (iota 10)))
#f
```

---

## `(remove-na lst)`

**returns:** `lst` with all `'na` elements removed.

```scheme
> (remove-na '(1 na 2 3))
(1 2 3)
> (remove-na '(1 NA 2 3))   ; 'NA is not missing
(1 NA 2 3)
> (remove-na '(1 "na" 2 3)) ; "na" is not missing
(1 "na" 2 3)
```

---

## `(dataframe-remove-na df [name ...])`

**returns:** a dataframe with rows containing `'na` removed. By default removes rows where any column is `'na`. Optionally restrict to specific column `name`s.

```scheme
> (define df
    (make-df*
      (a 1 2 3 4 'na)
      (b 'na 7 8 9 10)
      (c 11 12 'na 14 15)))

> (dataframe-display (dataframe-remove-na df))

 dim: 2 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      2.      7.     12.
      4.      9.     14.

;; Remove rows where only columns a or c are na
> (dataframe-display (dataframe-remove-na df 'a 'c))

 dim: 3 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.      na     11.
      2.       7     12.
      4.       9     14.
```
