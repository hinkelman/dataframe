# Reshape

## `(dataframe-stack df names names-to values-to)`

**returns:** a long-format dataframe by stacking the columns `names` from wide-format `df`. `names-to` is the name of the new column that will hold the original column names; `values-to` is the name of the new column that will hold the values.

```scheme
> (define df
    (make-df*
      (day 1 2)
      (hour 10 11)
      (a 97 78)
      (b 84 47)
      (c 55 54)))

> (dataframe-display (dataframe-stack df '(a b c) 'site 'count))

 dim: 6 rows x 4 cols
     day    hour    site   count
   <num>   <num>   <sym>   <num>
      1.     10.       a     97.
      2.     11.       a     78.
      1.     10.       b     84.
      2.     11.       b     47.
      1.     10.       c     55.
      2.     11.       c     54.
```

!!! tip "Stacking for aggregation"
    Long format is often the right shape for `dataframe-aggregate*`:

    ```scheme
    > (-> (make-df*
            (day 1 1 2 2)
            (hour 10 11 10 11)
            (a 97 78 83 80)
            (b 84 47 73 46)
            (c 55 54 38 58))
          (dataframe-stack '(a b c) 'site 'count)
          (dataframe-aggregate*
            (hour site)
            (total-count (count) (apply + count)))
          (dataframe-display))

     dim: 6 rows x 3 cols
        hour    site  total-count
       <num>   <sym>        <num>
         10.       a         180.
         11.       a         158.
         10.       b         157.
         11.       b          93.
         10.       c          93.
         11.       c         112.
    ```

---

## `(dataframe-spread df names-from values-from [fill-value])`

**returns:** a wide-format dataframe by spreading `df`. `names-from` is the column whose values become new column names; `values-from` is the column whose values are spread across those new columns. `fill-value` is used for missing combinations (default `'na`).

```scheme
> (define df1
    (make-df*
      (day 1 1 2)
      (grp "A" "B" "B")
      (val 10 20 30)))

> (dataframe-display (dataframe-spread df1 'grp 'val))

 dim: 2 rows x 3 cols
     day       A       B
   <num>   <num>   <num>
      1.      10     20.
      2.      na     30.

> (dataframe-display (dataframe-spread df1 'grp 'val 0))

 dim: 2 rows x 3 cols
     day       A       B
   <num>   <num>   <num>
      1.     10.     20.
      2.      0.     30.

;; multi-column key example
> (define df2
    (make-df*
      (day 1 1 1 1 2 2 2 2)
      (hour 10 10 11 11 10 10 11 11)
      (grp 'a 'b 'a 'b 'a 'b 'a 'b)
      (val 83 78 80 105 95 77 96 99)))

> (dataframe-display (dataframe-spread df2 'grp 'val))

 dim: 4 rows x 4 cols
     day    hour       a       b
   <num>   <num>   <num>   <num>
      1.     10.     83.     78.
      1.     11.     80.    105.
      2.     10.     95.     77.
      2.     11.     96.     99.
```
