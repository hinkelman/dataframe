# Modify & Aggregate

## Modify

### `(dataframe-modify df new-names names procedure ...)`

**returns:** a dataframe with new or replaced columns. Each `procedure` takes the columns in the corresponding `names` list as arguments. If a name in `new-names` already exists in `df`, that column is replaced; otherwise a new column is added. Expressions in later `procedure`s can reference columns created by earlier ones in the same call.

If `names` is empty `()`, `procedure` must return either a scalar (repeated to fill the column) or a list of length equal to the number of rows.

### `(dataframe-modify* df (new-name names expr) ...)`

**returns:** the same as `dataframe-modify` using a more concise implicit-lambda syntax. Each clause is `(new-name (col ...) expr)`.

```scheme
> (define df
    (make-df*
      (grp "a" "a" "b" "b" "b")
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display
    (dataframe-modify*
      df
      (grp (grp) (string-upcase grp))
      (total (adult juv) (+ adult juv))
      (prop-juv (juv total) (/ juv total))
      (scalar () 42)
      (lst () '(2 4 6 8 10))))

 dim: 5 rows x 8 cols
     grp     trt   adult     juv   total  prop-juv  scalar     lst
   <str>   <sym>   <num>   <num>   <num>     <num>   <num>   <num>
       A       a      1.     10.     11.     10/11     42.      2.
       A       b      2.     20.     22.     10/11     42.      4.
       B       a      3.     30.     33.     10/11     42.      6.
       B       b      4.     40.     44.     10/11     42.      8.
       B       b      5.     50.     55.     10/11     42.     10.
```

---

### `(dataframe-modify-at df procedure name ...)`

**returns:** a dataframe with the specified columns transformed by `procedure`, which must accept exactly one argument.

### `(dataframe-modify-all df procedure)`

**returns:** a dataframe with every column transformed by `procedure`.

```scheme
> (dataframe-display (dataframe-modify-at df symbol->string 'grp 'trt))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv
   <str>   <str>   <num>   <num>
       a       a      1.     10.
       a       b      2.     20.
       b       a      3.     30.
       b       b      4.     40.
       b       b      5.     50.

> (dataframe-display
    (dataframe-modify-all (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9))
                          (lambda (x) (* x 100))))

 dim: 3 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
    100.    400.    700.
    200.    500.    800.
    300.    600.    900.
```

---

## Aggregate

### `(dataframe-aggregate df group-names new-names names procedure ...)`

**returns:** a dataframe where `df` is split by `group-names` and each group is summarised by applying each `procedure` to the corresponding `names` columns.

### `(dataframe-aggregate* df group-names (new-name names expr) ...)`

**returns:** the same using implicit-lambda syntax. Each clause is `(new-name (col ...) expr)`.

```scheme
> (define df
    (make-df*
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display
    (dataframe-aggregate*
      df
      (grp)
      (adult-sum (adult) (sum adult))
      (juv-sum (juv) (sum juv))))

 dim: 2 rows x 3 cols
     grp  adult-sum  juv-sum
   <sym>      <num>    <num>
       a         3.      30.
       b        12.     120.

;; multiple grouping columns
> (dataframe-display
    (dataframe-aggregate*
      df
      (grp trt)
      (adult-sum (adult) (sum adult))
      (juv-sum (juv) (sum juv))))

 dim: 4 rows x 4 cols
     grp     trt  adult-sum  juv-sum
   <sym>   <sym>      <num>    <num>
       a       a         1.      10.
       a       b         2.      20.
       b       a         3.      30.
       b       b         9.      90.
```
