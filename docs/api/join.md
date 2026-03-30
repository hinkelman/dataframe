# Join

All join procedures require that the non-join columns across `df1` and `df2` have unique names.

## `(dataframe-inner-join df1 df2 join-names)`

**returns:** a dataframe containing only rows that match in both `df1` and `df2`, joined on the list of column names `join-names`.

## `(dataframe-left-join df1 df2 join-names [fill-value])`

**returns:** a dataframe with all rows from `df1`, augmented with matching columns from `df2`. Rows in `df1` with no match in `df2` are filled with `fill-value` (default `'na`).

## `(dataframe-left-join-all dfs join-names [fill-value])`

**returns:** a dataframe produced by recursively left-joining each dataframe in the list `dfs` onto the previous one. `fill-value` defaults to `'na`.

```scheme
> (define df1
    (make-df*
      (site "b" "a" "c")
      (habitat "grassland" "meadow" "woodland")))

> (define df2
    (make-df*
      (site "c" "b" "c" "b" "d")
      (day 1 1 2 2 1)
      (catch 10 12 20 24 100)))

> (dataframe-display (dataframe-left-join df1 df2 '(site)))

 dim: 5 rows x 4 cols
    site    habitat     day   catch
   <str>      <str>   <num>   <num>
       b  grassland       1      12
       b  grassland       2      24
       a     meadow      na      na
       c   woodland       1      10
       c   woodland       2      20

> (dataframe-display (dataframe-inner-join df1 df2 '(site)))

 dim: 4 rows x 4 cols
    site    habitat     day   catch
   <str>      <str>   <num>   <num>
       b  grassland      1.     12.
       b  grassland      2.     24.
       c   woodland      1.     10.
       c   woodland      2.     20.

> (define df3
    (make-df*
      (first "sam" "bob" "bob" "dan")
      (last "son" "ert" "ert" "man")
      (age 10 20 20 40)))

> (define df4
    (make-df*
      (first "sam" "bob" "bob" "dan")
      (last "son" "ert" "ert" "man")
      (game 1 1 2 1)
      (goals 0 1 3 2)))

> (dataframe-display (dataframe-left-join df4 df3 '(first last)))

 dim: 4 rows x 5 cols
   first    last    game   goals     age
   <str>   <str>   <num>   <num>   <num>
     sam     son      1.      0.     10.
     bob     ert      1.      1.     20.
     bob     ert      2.      3.     20.
     dan     man      1.      2.     40.
```
