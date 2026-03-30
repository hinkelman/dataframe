# Sort

## `(dataframe-sort df predicates names)`

**returns:** a dataframe with rows sorted by applying the list of `predicates` to the corresponding list of column `names`. Multiple sort keys are applied left to right.

## `(dataframe-sort* df (predicate name) ...)`

**returns:** a dataframe with rows sorted by the `(predicate name)` pairs. Equivalent to `dataframe-sort` but uses a more concise paired syntax.

```scheme
> (define df
    (make-df*
      (grp "a" "a" "b" "b" "b")
      (trt "a" "b" "a" "b" "b")
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

;; Single sort key
> (dataframe-display (dataframe-sort df (list string>?) '(trt)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <str>   <str>   <num>   <num>
       a       b      2.     20.
       b       b      4.     40.
       b       b      5.     50.
       a       a      1.     10.
       b       a      3.     30.

> (dataframe-display (dataframe-sort* df (string>? trt)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <str>   <str>   <num>   <num>
       a       b      2.     20.
       b       b      4.     40.
       b       b      5.     50.
       a       a      1.     10.
       b       a      3.     30.

;; Multiple sort keys
> (dataframe-display (dataframe-sort df (list string>? >) '(trt adult)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <str>   <str>   <num>   <num>
       b       b      5.     50.
       b       b      4.     40.
       a       b      2.     20.
       b       a      3.     30.
       a       a      1.     10.

> (dataframe-display (dataframe-sort* df (string>? trt) (> adult)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <str>   <str>   <num>   <num>
       b       b      5.     50.
       b       b      4.     40.
       a       b      2.     20.
       b       a      3.     30.
       a       a      1.     10.
```
