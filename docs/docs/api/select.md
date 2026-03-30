# Select, Drop & Rename

## Select

### `(dataframe-select df names)`

**returns:** a dataframe containing only the columns in the list `names`.

### `(dataframe-select* df name ...)`

**returns:** a dataframe containing only the specified `name`s (bare symbols, not a list).

```scheme
> (define df (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9)))

> (dataframe-display (dataframe-select df '(a c)))

 dim: 3 rows x 2 cols
       a       c
   <num>   <num>
      1.      7.
      2.      8.
      3.      9.

> (dataframe-display (dataframe-select* df a c))

 dim: 3 rows x 2 cols
       a       c
   <num>   <num>
      1.      7.
      2.      8.
      3.      9.
```

---

## Drop

### `(dataframe-drop df names)`

**returns:** a dataframe with the columns in the list `names` removed.

### `(dataframe-drop* df name ...)`

**returns:** a dataframe with the specified `name`s removed (bare symbols, not a list).

```scheme
> (dataframe-display (dataframe-drop df '(b)))

 dim: 3 rows x 2 cols
       a       c
   <num>   <num>
      1.      7.
      2.      8.
      3.      9.
```

---

## Rename

### `(dataframe-rename df old-names new-names)`

**returns:** a dataframe with columns renamed from `old-names` to `new-names` (both lists of symbols).

### `(dataframe-rename* df (old-name new-name) ...)`

**returns:** a dataframe with columns renamed using bare `(old new)` pairs. Silently ignores pairs where `old-name` is not found.

### `(dataframe-rename-all df new-names)`

**returns:** a dataframe with all columns renamed to `new-names` (a list of symbols, must match column count).

```scheme
> (define df (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9)))

> (dataframe-display (dataframe-rename df '(a b) '(A B)))

 dim: 3 rows x 3 cols
       A       B       c
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.

> (dataframe-display (dataframe-rename* df (a A) (b B)))

 dim: 3 rows x 3 cols
       A       B       c
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.

;; no change when old name is not found
> (dataframe-display (dataframe-rename* df (d Dee)))
 dim: 3 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.

> (dataframe-display (dataframe-rename-all df '(A B C)))

 dim: 3 rows x 3 cols
       A       B       C
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.
```
