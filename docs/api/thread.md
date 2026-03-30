# Thread Macros

These macros make it easier to chain a sequence of operations on a dataframe without deeply nested expressions.

## `(-> expr ...)`

**returns:** the result of threading `expr` through each subsequent form by inserting it as the **first** argument.

## `(->> expr ...)`

**returns:** the result of threading `expr` through each subsequent form by inserting it as the **last** argument.

```scheme
;; Simple scalar pipeline
> (-> '(1 2 3)
      (mean)
      (+ 10))
12

;; Dataframe pipeline using ->
> (-> (make-df*
        (grp 'a 'a 'b 'b 'b)
        (trt 'a 'b 'a 'b 'b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (dataframe-modify*
        (total (adult juv) (+ adult juv)))
      (dataframe-display))

 dim: 5 rows x 5 cols
     grp     trt   adult     juv   total
   <sym>   <sym>   <num>   <num>   <num>
       a       a      1.     10.     11.
       a       b      2.     20.     22.
       b       a      3.     30.     33.
       b       b      4.     40.     44.
       b       b      5.     50.     55.

;; Mixing -> and ->>
;; ->> is used where the dataframe needs to be the last argument (e.g. map)
> (-> (make-df*
        (grp 'a 'a 'b 'b 'b)
        (trt 'a 'b 'a 'b 'b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (dataframe-split 'grp)
      (->> (map (lambda (df)
                  (dataframe-modify*
                    df
                    (juv-mean () (mean ($ df 'juv)))))))
      (->> (dataframe-bind-all))
      (dataframe-filter* (juv juv-mean) (> juv juv-mean))
      (dataframe-display))

 dim: 2 rows x 5 cols
     grp     trt   adult     juv  juv-mean
   <sym>   <sym>   <num>   <num>     <num>
       a       b      2.     20.       15.
       b       b      5.     50.       40.
```

!!! tip
    Use `->` for most dataframe operations (which take the dataframe as the first argument). Switch to `->>` when passing the result into a higher-order function like `map`, where the dataframe is the last argument.
