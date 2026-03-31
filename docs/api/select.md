# Select & Drop

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
