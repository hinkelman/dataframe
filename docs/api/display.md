# Display

## `(dataframe-display df [n total-width min-width])`

**displays:** the dataframe `df` to standard output, up to `n` rows and as many columns as fit within `total-width` characters, using `min-width` as the minimum column width.

| Parameter | Default |
|---|---|
| `n` | `10` |
| `total-width` | `76` |
| `min-width` | `7` |

```scheme
> (define df
    (make-df*
      (a 1 2 3)
      (b 4 5 6)
      (c 7 8 9)))

> (dataframe-display df)

 dim: 3 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.      4.      7.
      2.      5.      8.
      3.      6.      9.
```

---

## `(dataframe-glimpse df [total-width])`

**displays:** a transposed view of the dataframe where each row is a column, showing the column name, type, and as many values as fit in `total-width`. Useful for wide dataframes.

| Parameter | Default |
|---|---|
| `total-width` | `76` |

```scheme
> (define df2
    (make-df*
      (a 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
      (b 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25)))

> (dataframe-glimpse df2)

 dim: 25 rows x 2 cols
 a       <num>   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, ...
 b       <num>   1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, ...
```
