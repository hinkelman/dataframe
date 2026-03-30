# Filter

## `(dataframe-unique df)`

**returns:** a dataframe with duplicate rows removed.

```scheme
> (define df
    (make-df*
      (Name "Peter" "Paul" "Mary" "Peter")
      (Pet "Rabbit" "Cat" "Dog" "Rabbit")))

> (dataframe-display (dataframe-unique df))

 dim: 3 rows x 2 cols
    Name     Pet
   <str>   <str>
   Peter  Rabbit
    Paul     Cat
    Mary     Dog
```

---

## `(dataframe-filter df names procedure)`

**returns:** a dataframe with rows kept where `procedure` applied to the columns `names` returns `#t`.

## `(dataframe-filter* df names expr)`

**returns:** a dataframe with rows kept where `expr` (applied to `names`) is truthy. The `*` variant uses implicit lambda — `names` can be used directly in `expr`.

```scheme
> (define df
    (make-df*
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display (dataframe-filter df '(adult) (lambda (adult) (> adult 3))))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       b      4.     40.
       b       b      5.     50.

> (dataframe-display (dataframe-filter* df (adult) (> adult 3)))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       b      4.     40.
       b       b      5.     50.

> (dataframe-display
    (dataframe-filter* df (grp juv) (and (symbol=? grp 'b) (< juv 50))))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       a      3.     30.
       b       b      4.     40.
```

---

## `(dataframe-filter-at df procedure name ...)`

**returns:** a dataframe with rows kept where `procedure` returns `#t` for all specified columns.

## `(dataframe-filter-all df procedure)`

**returns:** a dataframe with rows kept where `procedure` returns `#t` for every column.

```scheme
> (define df
    (make-df*
      (a 1 'na 3)
      (b 'na 5 6)
      (c 7 'na 9)))

> (dataframe-display (dataframe-filter-at df number? 'a 'c))

 dim: 2 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      1.      na      7.
      3.       6      9.

> (dataframe-display (dataframe-filter-all df number?))

 dim: 1 rows x 3 cols
       a       b       c
   <num>   <num>   <num>
      3.      6.      9.
```

---

## `(dataframe-partition df names procedure)`

**returns:** two dataframes — rows matching `procedure` and rows not matching.

## `(dataframe-partition* df names expr)`

**returns:** two dataframes using the implicit-lambda `*` syntax.

```scheme
> (define-values (keep drop)
    (dataframe-partition df '(adult) (lambda (adult) (> adult 3))))

> (define-values (keep* drop*)
    (dataframe-partition* df (adult) (> adult 3)))

> (dataframe-display keep)

 dim: 2 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       b       b      4.     40.
       b       b      5.     50.

> (dataframe-display drop)

 dim: 3 rows x 4 cols
     grp     trt   adult     juv
   <sym>   <sym>   <num>   <num>
       a       a      1.     10.
       a       b      2.     20.
       b       a      3.     30.
```
