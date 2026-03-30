# Split, Bind & Append

## `(dataframe-split df group-name ...)`

**returns:** a list of dataframes, one per unique combination of values in `group-name` column(s). Requires that all values in each grouping column are the same type.

```scheme
> (define df
    (make-df*
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (for-each dataframe-display (dataframe-split df 'grp))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       a       a      1.     10.
       a       b      2.     20.

 dim: 3 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       a      3.     30.
       b       b      4.     40.
       b       b      5.     50.
```

---

## `(dataframe-bind df1 df2 [fill-value])`

**returns:** a dataframe with the rows of `df2` appended below `df1`. Columns not present in one dataframe are filled with `fill-value` (default `'na`). Column order follows first appearance across both dataframes.

## `(dataframe-bind-all dfs [fill-value])`

**returns:** the same as `dataframe-bind` but for a list of dataframes `dfs`.

```scheme
> (define df1 (make-df* (a 1 2 3) (b 10 20 30) (c 100 200 300)))
> (define df2 (make-df* (a 4 5 6) (b 40 50 60)))

> (dataframe-display (dataframe-bind df1 df2))

 dim: 6 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.     10.     100
      2.     20.     200
      3.     30.     300
      4.     40.      na
      5.     50.      na
      6.     60.      na

> (dataframe-display (dataframe-bind df1 df2 -999))

 dim: 6 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.     10.    100.
      2.     20.    200.
      3.     30.    300.
      4.     40.   -999.
      5.     50.   -999.
      6.     60.   -999.

;; split-then-bind round-trip
> (dataframe-display (dataframe-bind-all (dataframe-split df 'grp 'trt)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       a       a      1.     10.
       a       b      2.     20.
       b       a      3.     30.
       b       b      4.     40.
       b       b      5.     50.
```

---

## `(dataframe-append df1 df2 ...)`

**returns:** a dataframe with the columns of `df2 ...` appended to the right of `df1`. All dataframes must have the same number of rows, and column names must be unique across all inputs.

```scheme
> (define df1 (make-df* (a 1 2 3) (b 4 5 6)))
> (define df2 (make-df* (c 7 8 9) (d 10 11 12)))

> (dataframe-display (dataframe-append df1 df2))

 dim: 3 rows x 4 cols
       a       b       c       d
   <num>   <num>   <num>   <num>
      1.      4.      7.     10.
      2.      5.      8.     11.
      3.      6.      9.     12.
```
