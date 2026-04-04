# Join

All join procedures require that the non-join columns across `df1` and `df2` have unique names.

## Specifying join columns

All join procedures accept `join-names` in three forms:

- **Omitted**: join on all column names common to both dataframes; raises an error if no common names exist
- **List of symbols** `'(a b)`: join on columns with the same name in both dataframes
- **List of pairs** `'((a . x) (b . y))`: join on columns with different names in each dataframe; the result uses the names from `df1`

## `(dataframe-inner-join df1 df2 [join-names])`

**returns:** a dataframe containing only rows that match in both `df1` and `df2`.

## `(dataframe-full-join df1 df2 [join-names fill-value])`

**returns:** a dataframe with all rows from both `df1` and `df2`. Matched rows are combined; unmatched rows from either side are filled with `fill-value` (default `'na`). Unmatched rows from `df1` appear first, followed by unmatched rows from `df2`.

## `(dataframe-left-join df1 df2 [join-names fill-value])`

**returns:** a dataframe with all rows from `df1`, augmented with matching columns from `df2`. Rows in `df1` with no match in `df2` are filled with `fill-value` (default `'na`).

## `(dataframe-left-join-all dfs [join-names fill-value])`

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

;; left join: "a" has no match in df2, so day and catch are filled with 'na
;; "d" exists only in df2 and is dropped
> (dataframe-display (dataframe-left-join df1 df2 '(site)))

 dim: 5 rows x 4 cols
    site    habitat     day   catch
   <str>      <str>   <num>   <num>
       b  grassland       1      12
       b  grassland       2      24
       a     meadow      na      na
       c   woodland       1      10
       c   woodland       2      20

;; inner join: only sites present in both are retained
> (dataframe-display (dataframe-inner-join df1 df2 '(site)))

 dim: 4 rows x 4 cols
    site    habitat     day   catch
   <str>      <str>   <num>   <num>
       b  grassland      1.     12.
       b  grassland      2.     24.
       c   woodland      1.     10.
       c   woodland      2.     20.

;; full join: all sites from both; "a" gets na for day/catch, "d" gets na for habitat
> (dataframe-display (dataframe-full-join df1 df2 '(site)))

 dim: 6 rows x 4 cols
    site    habitat     day   catch
   <str>      <str>   <num>   <num>
       b  grassland       1      12
       b  grassland       2      24
       a     meadow      na      na
       c   woodland       1      10
       c   woodland       2      20
       d         na       1     100

;; omitting join-names auto-detects 'site as the only common column
> (dataframe-equal?
    (dataframe-left-join df1 df2)
    (dataframe-left-join df1 df2 '(site)))
#t

;; two-column key
> (define df3
    (make-df*
      (first "sam" "bob" "sam" "dan")
      (last  "son" "ert" "jam" "man")
      (age 10 20 30 40)))

> (define df4
    (make-df*
      (first "sam" "bob" "dan" "bob")
      (last "son" "ert" "man" "ert")
      (game 1 1 1 2)
      (goals 0 1 2 3)))

> (dataframe-display (dataframe-left-join df3 df4 '(first last)))

 dim: 5 rows x 5 cols
   first    last     age    game   goals
   <str>   <str>   <num>   <num>   <num>
     sam     son      10       1       0
     bob     ert      20       1       1
     bob     ert      20       2       3
     sam     jam      30      na      na
     dan     man      40       1       2

;; cross-name join: df2's 'location column corresponds to df1's 'site column
> (define df5
    (make-df*
      (location "c" "b" "c" "b" "d")
      (day 1 1 2 2 1)
      (catch 10 12 20 24 100)))

> (dataframe-equal?
    (dataframe-left-join df1 df5 '((site . location)))
    (dataframe-left-join df1 df2 '(site)))
#t

;; custom fill-value
> (dataframe-display (dataframe-left-join df2 df1 '(site) -999))

 dim: 5 rows x 4 cols
    site     day   catch    habitat
   <str>   <num>   <num>      <str>
       c       1      10   woodland
       c       2      20   woodland
       b       1      12  grassland
       b       2      24  grassland
       d       1     100      -999
```
