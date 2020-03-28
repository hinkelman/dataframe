![](https://github.com/hinkelman/dataframe/workflows/Master/badge.svg)

# Chez Scheme Dataframe Library

A dataframe record type for Chez Scheme with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, modify, and aggregate dataframes. 

Related blog posts:  
[A dataframe record type for Chez Scheme](https://www.travishinkelman.com/posts/dataframe-record-type-for-chez-scheme/)  

## Installation and Import

```
$ cd ~/scheme # where '~/scheme' is the path to your Chez Scheme libraries
$ git clone git://github.com/hinkelman/dataframe.git
```

For more information on installing Chez Scheme libraries, see blog posts for [macOS and Windows](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs/) or [Ubuntu](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs-ubuntu/).

Import all `dataframe` procedures: `(import (dataframe df))`

## Table of Contents  

### Dataframe record type  

[`(make-dataframe alist)`](#make-df)  
[`(dataframe-contains df name ...)`](#df-contains)  
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe-write df path overwrite?)`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe->rowtable df)`](#df-rows)  
[`(rowtable->dataframe rt header?)`](#rows-df)  
[`(dataframe-ref df indices name ...)`](#df-ref)  
[`(dataframe-values df name)`](#df-values)  

### Select, drop, and rename columns  

[`(dataframe-select df name ...)`](#df-select)  
[`(dataframe-drop df name ...)`](#df-drop)  
[`(dataframe-names-update df names)`](#df-names-update)  
[`(dataframe-rename df name-pairs)`](#df-rename)  

### Filter and sort  

[`(dataframe-unique df)`](#df-unique)  
[`(filter-expr (names) (expr))`](#filter-expr)  
[`(dataframe-filter df filter-expr)`](#df-filter)  
[`(dataframe-partition df filter-expr)`](#df-partition)  
[`(sort-expr (predicate name) ...)`](#sort-expr)  
[`(dataframe-sort df sort-expr)`](#df-sort)  

### Split, bind, and append  

[`(dataframe-split df group-name ...)`](#df-split)  
[`(dataframe-bind df1 df2 ...)`](#df-bind)  
[`(dataframe-bind-all missing-value df1 df2 ...)`](#df-bind-all)  
[`(dataframe-append df1 df2 ...)`](#df-append)  

### Modify and aggregate  

[`(modify-expr (new-name (names) (expr)) ...)`](#modify-expr)  
[`(dataframe-modify df modify-expr)`](#df-modify)  
[`(aggregate-expr (new-name (names) (expr)) ...)`](#aggregate-expr)  
[`(dataframe-aggregate df group-names aggregate-expr)`](#df-aggregate)  

### Thread first and thread last  

[`(-> expr ...)`](#thread-first)  
[`(->> expr ...)`](#thread-last)  

## Dataframe record type  

#### <a name="make-df"></a> procedure: `(make-dataframe alist)`  
**returns:** a dataframe record type with three fields: alist, names, and dim  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6))))

> df
#[#{dataframe cziqfonusl4ihl0gdwa8clop7-3} ((a 1 2 3) (b 4 5 6)) (a b) (3 . 2)]

> (dataframe? df)
#t

> (dataframe? '((a 1 2 3) (b 4 5 6)))
#f

> (dataframe-alist df)
((a 1 2 3) (b 4 5 6))

> (dataframe-names df)
(a b)

> (dataframe-dim df)
(3 . 2)                  ; (rows . columns)

> (define df (make-dataframe '(("a" 1 2 3) ("b" 4 5 6))))

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-contains"></a> procedure: `(dataframe-contains df name ...)`  
**returns:** `#t` if all column `names` are found in dataframe `df`, `#f` otherwise  

```
> (define df (make-dataframe '((a 1) (b 2) (c 3) (d 4))))

> (dataframe-contains? df 'a 'c 'd)
#t

> (dataframe-contains? df 'b 'e)
#f
```

#### <a name="df-head"></a> procedure: `(dataframe-head df n)`  
**returns:** a dataframe with first `n` rows from dataframe `df`  

#### <a name="df-tail"></a> procedure: `(dataframe-tail df n)`  
**returns:** a dataframe with the `n`th tail (zero-based) rows from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))

> (dataframe-head df 3)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]

> (dataframe-tail df 2)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 3 1 2 3) (b 6 4 5 6) (c 9 -999 -999 -999)) (a b c) (4 . 3)]
```

#### <a name="df-equal"></a> procedure: `(dataframe-equal? df1 df2 ...)`  
**returns:** `#t` if all dataframes are equal, `#f` otherwise  

```
> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((b 4 5 6) (a 1 2 3))))
#f

> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((a 10 2 3) (b 4 5 6))))
#f
```

#### <a name="df-write"></a> procedure: `(dataframe-write df path overwrite?)`  
**writes:** a dataframe `df` as a Scheme object to `path`; if file exists at `path`, operation will fail unless `overwrite?` is #t  

#### <a name="df-read"></a> procedure: `(dataframe-read path)`  
**returns:** a dataframe read from `path`

```
> (define df (make-dataframe '((grp "b" "b" "a" "b" "a")
                               (trt b b b a a)
                               (adult 5 4 2 3 1)
                               (juv 50 40 20 30 10))))

> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3}
  ((grp "b" "b" "a" "b" "a")
   (trt b b b a a)
   (adult 5 4 2 3 1)
   (juv 50 40 20 30 10))
  (grp trt adult juv) (5 . 4)]

> (dataframe-write df "df-example.scm" #t)

> (define df2 (dataframe-read "df-example.scm"))

> df2
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3}
  ((grp "b" "b" "a" "b" "a")
   (trt b b b a a)
   (adult 5 4 2 3 1)
   (juv 50 40 20 30 10))
  (grp trt adult juv) (5 . 4)]
```

#### <a name="df-rows"></a> procedure: `(dataframe->rowtable df)`  
**returns:** a rowtable from dataframe `df`

```
;; a dataframe is a column-based data structure; a rowtable is a row-based data structure

> (define df (make-dataframe '((a 100 300) (b 4 6) (c 700 900))))

> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 100 300) (b 4 6) (c 700 900)) (a b c) (2 . 3)]

> (dataframe->rowtable df)
((a b c) (100 4 700) (300 6 900))
```

#### <a name="rows-df"></a> procedure: `(rowtable->dataframe rt header?)`  
**returns:** a dataframe from rowtable `rt`; if `header?` is `#f` a header row is created

```
;; a rowtable is a row-based data structure; a dataframe is a column-based data structure

> (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9)) #t)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]

> (rowtable->dataframe '((1 4 7) (2 5 8) (3 6 9)) #f)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((V0 1 2 3) (V1 4 5 6) (V2 7 8 9)) (V0 V1 V2) (3 . 3)]

> (rowtable->dataframe '(("a" "b" "c") (1 4 7) (2 5 8) (3 6 9)) #t)

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-ref"></a> procedure: `(dataframe-ref df indices name ...)`  
**returns:** a dataframe with rows specified by `indices` (zero-based) from dataframe `df`; optionally, can specify column `names` to return; defaults to all columns  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-ref df '(0 2 4))
#[#{dataframe blgxd6z9um6m4hqkq6eorahbs-3}
  ((grp "a" "b" "b")
   (trt "a" "a" "b")
   (adult 1 3 5)
   (juv 10 30 50))
  (grp trt adult juv) (3 . 4)]

> (dataframe-ref df '(0 2 4) 'adult 'juv)
#[#{dataframe blgxd6z9um6m4hqkq6eorahbs-3} ((adult 1 3 5) (juv 10 30 50)) (adult juv) (3 . 2)]
```

#### <a name="df-values"></a> procedure: `(dataframe-values df name)`  
**returns:** a list of values for column `name` from dataframe `df`  

```
> (define df (make-dataframe '((a 100 200 300) (b 4 5 6) (c 700 800 900))))

> (dataframe-values df 'b)
(4 5 6)

> ($ df 'b)                   ; $ is shorthand for dataframe-values; inspired by R, e.g., df$b.
(4 5 6)

> (map (lambda (name) ($ df name)) '(c a))
((700 800 900) (100 200 300))
```

## Select, drop, and rename columns  

#### <a name="df-select"></a> procedure: `(dataframe-select df name ...)`  
**returns:** a dataframe of columns with `names` selected from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-select df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]

> (dataframe-select df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((c 7 8 9) (b 4 5 6)) (c b) (3 . 2)]
```

#### <a name="df-drop"></a> procedure: `(dataframe-drop df name ...)`  
**returns:** a dataframe of columns with `names` dropped from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-drop df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]

> (dataframe-drop df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((b 4 5 6) (c 7 8 9)) (b c) (3 . 2)]
```

#### <a name="df-names-update"></a> procedure: `(dataframe-names-update df names)`  
**returns:** a dataframe with `names` replacing column names from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-names-update df '(A B C))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((A 1 2 3) (B 4 5 6) (C 7 8 9)) (A B C) (3 . 3)]

> (dataframe-names-update df '(A B C D))

Exception in (dataframe-names-update df names): names length must be 3, not 4
```

#### <a name="df-rename"></a> procedure: `(dataframe-rename df name-pairs)`  
**returns:** a dataframe with column names from dataframe `df` renamed according to `name-pairs`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-rename df '((b Bee) (c Sea)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((a 1 2 3) (Bee 4 5 6) (Sea 7 8 9)) (a Bee Sea) (3 . 3)]
```

## Filter and sort  

#### <a name="df-unique"></a> procedure: `(dataframe-unique df)`  
**returns:** a dataframe with only the unique rows of dataframe `df`  

```
> (define df (make-dataframe '((Name "Peter" "Paul" "Mary" "Peter")
                               (Pet "Rabbit" "Cat" "Dog" "Rabbit"))))

> (dataframe-unique df)
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3}
  ((Name "Paul" "Mary" "Peter")
   (Pet "Cat" "Dog" "Rabbit"))
  (Name Pet) (3 . 2)]

> (define df2 (make-dataframe '((grp a a b b b)
                                (trt a b a b b)
                                (adult 1 2 3 4 5)
                                (juv 10 20 30 40 50))))

> (dataframe-unique (dataframe-select df2 'grp 'trt))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp a a b b) (trt a b a b)) (grp trt) (4 . 2)]
```

#### <a name="filter-expr"></a> procedure: `(filter-expr (names) (expr))`  
**returns:** a list where the first element is a list of column `names` and the second element is a lambda procedure based on `expr`  

```
> (filter-expr (adult) (> adult 3))
((adult) #<procedure>)

> (filter-expr (grp juv) (and (symbol=? grp 'b) (< juv 50)))
((grp juv) #<procedure>)
```

#### <a name="df-filter"></a> procedure: `(dataframe-filter df filter-expr)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered according to the `filter-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-filter df (filter-expr (adult) (> adult 3)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3}
  ((grp b b)
   (trt b b)
   (adult 4 5)
   (juv 40 50))
  (grp trt adult juv) (2 . 4)]

> (dataframe-filter df (filter-expr (grp juv) (and (symbol=? grp 'b) (< juv 50))))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3}
  ((grp b b)
   (trt a b)
   (adult 3 4)
   (juv 30 40))
  (grp trt adult juv) (2 . 4)]
```

#### <a name="df-partition"></a> procedure: `(dataframe-partition df filter-expr)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned according to the `filter-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-partition df (filter-expr (adult) (> adult 3)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp b b) (trt b b) (adult 4 5) (juv 40 50)) (grp trt adult juv) (2 . 4)]
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp a a b) (trt a b a) (adult 1 2 3) (juv 10 20 30)) (grp trt adult juv) (3 . 4)]
```

#### <a name="sort-expr"></a> procedure: `(sort-expr (predicate name) ...)`  
**returns:** a list where the first element is a list of `predicate` procedures and the second element is a list of column `names`  

```
> (sort-expr (string<? trt))
((#<procedure string<?>) (trt))

> (sort-expr (string<? trt) (< adult))
((#<procedure string<?> #<procedure <>) (trt adult))
```

#### <a name="df-sort"></a> procedure: `(dataframe-sort df sort-expr)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according to the `sort-expr`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-sort df (sort-expr (string<? trt)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3}
  ((grp "a" "b" "b" "a" "b")
   (trt "b" "b" "b" "a" "a")
   (adult 2 4 5 1 3)
   (juv 20 40 50 10 30))
  (grp trt adult juv) (5 . 4)]

> (dataframe-sort df (sort-expr (string<? trt) (< adult)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3}
  ((grp "b" "b" "a" "b" "a")
   (trt "b" "b" "b" "a" "a")
   (adult 5 4 2 3 1)
   (juv 50 40 20 30 10))
  (grp trt adult juv) (5 . 4)]
```

## Split, bind, and append  

#### <a name="df-split"></a> procedure: `(dataframe-split df group-names ...)`  
**returns:** list of dataframes split into unique groups by `group-names` from dataframe `df`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-split df 'grp)
(#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a" "a") (trt "a" "b") (adult 1 2) (juv 10 20)) (grp trt adult juv) (2 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b" "b" "b") (trt "a" "b" "b") (adult 3 4 5) (juv 30 40 50)) (grp trt adult juv) (3 . 4)])
  
> (dataframe-split df 'grp 'trt)
(#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a") (trt "a") (adult 1) (juv 10)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a") (trt "b") (adult 2) (juv 20)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b") (trt "a") (adult 3) (juv 30)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b" "b") (trt "b" "b") (adult 4 5) (juv 40 50)) (grp trt adult juv) (2 . 4)])
```

#### <a name="df-bind"></a> procedure: `(dataframe-bind df1 df2 ...)`  
**returns:** a dataframe formed by binding only shared columns of the dataframes `df1 df2 ...`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (apply dataframe-bind (dataframe-split df 'grp 'trt))
#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3}
  ((grp "a" "a" "b" "b" "b")
   (trt "a" "b" "a" "b" "b")
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50))
  (grp trt adult juv) (5 . 4)]

> (define df1 (make-dataframe '((a 1 2 3) (b 10 20 30) (c 100 200 300))))

> (define df2 (make-dataframe '((a 4 5 6) (b 40 50 60))))

> (dataframe-bind df1 df2)
#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((a 1 2 3 4 5 6) (b 10 20 30 40 50 60)) (a b) (6 . 2)]

> (dataframe-bind df2 df1)
#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((a 4 5 6 1 2 3) (b 40 50 60 10 20 30)) (a b) (6 . 2)]
```

#### <a name="df-bind-all"></a> procedure: `(dataframe-bind-all missing-value df1 df2 ...)`  
**returns:** a dataframe formed by binding all columns of the dataframes `df1 df2 ...` where `missing-value` is used to fill values for columns that are not common to all dataframes  

```
> (define df1 (make-dataframe '((a 1 2 3) (b 10 20 30) (c 100 200 300))))

> (define df2 (make-dataframe '((a 4 5 6) (b 40 50 60))))

> (dataframe-bind-all -999 df1 df2)
#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3}
  ((a 1 2 3 4 5 6)
   (b 10 20 30 40 50 60)
   (c 100 200 300 -999 -999 -999))
  (a b c) (6 . 3)]

> (dataframe-bind-all -999 df2 df1)
#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3}
  ((a 4 5 6 1 2 3)
   (b 40 50 60 10 20 30)
   (c -999 -999 -999 100 200 300))
  (a b c) (6 . 3)]
```

#### <a name="df-append"></a> procedure: `(dataframe-append df1 df2 ...)`  
**returns:** a dataframe formed by appending columns of the dataframes `df1 df2 ...`  

```
> (define df1 (make-dataframe '((a 1 2 3) (b 4 5 6))))

> (define df2 (make-dataframe '((c 7 8 9) (d 10 11 12))))

> (dataframe-append df1 df2)
#[#{dataframe o6ydlodurwb4p6ft40uetno4d-3}
  ((a 1 2 3)
   (b 4 5 6)
   (c 7 8 9)
   (d 10 11 12))
  (a b c d) (3 . 4)]
  
> (dataframe-append df2 df1)
#[#{dataframe o6ydlodurwb4p6ft40uetno4d-3}
  ((c 7 8 9)
   (d 10 11 12)
   (a 1 2 3)
   (b 4 5 6))
  (c d a b) (3 . 4)]
```

## Modify and aggregate  

#### <a name="modify-expr"></a> procedure: `(modify-expr (new-name (names) (expr)) ...)`  
**returns:** a list where the first element is a list of new column names `new-name`, the second element is a list of lists of column `names`, and the third element is list of lambda procedures based on `expr`  

```
> (modify-expr (grp (grp) (symbol->string grp))
               (total (adult juv) (+ adult juv)))
((grp total) ((grp) (adult juv)) (#<procedure> #<procedure>))
```

#### <a name="df-modify"></a> procedure: `(dataframe-modify df modify-expr)`  
**returns:** a dataframe where the columns of dataframe `df` are modified or added according to the `modify-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
;; if new name occurs in dataframe, then column is replaced
;; if not, then new column is added

;; if names is empty, 
;;   and expr is a scalar, then the scalar is repeated to match the number of rows in the dataframe
;;   and expr is a list of length equal to number of rows in dataframe, then the list is used as a column

> (dataframe-modify df (modify-expr (grp (grp) (symbol->string grp))
                                    (total (adult juv) (+ adult juv))
                                    (scalar () 42)
                                    (lst () '(2 4 6 8 10))))  
#[#{dataframe dqmpcr7n11a0dimehuppd0gd9-3}
  ((grp "a" "a" "b" "b" "b")
   (trt a b a b b)
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50)
   (total 11 22 33 44 55)
   (scalar 42 42 42 42 42)
   (lst 2 4 6 8 10))
  (grp trt adult juv total scalar lst) (5 . 7)]
```

#### <a name="aggregate-expr"></a> procedure: `(aggregate-expr (new-name (names) (expr)) ...)`  
**returns:** a list where the first element is a list of new column names `new-name`, the second element is a list of lists of column `names`, and the third element is list of lambda procedures based on `expr`  

```
> (aggregate-expr (adult-sum (adult) (apply + adult))
                (juv-sum (juv) (apply + juv)))
((adult-sum juv-sum) ((adult) (juv)) (#<procedure> #<procedure>))
```

#### <a name="df-aggregate"></a> procedure: `(dataframe-aggregate df group-names aggregate-expr)`  
**returns:** a dataframe where the dataframe `df` was split according to list of `group-names` and aggregated according to `aggregate-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
> (dataframe-aggregate df
                       '(grp)
                       (aggregate-expr (adult-sum (adult) (apply + adult))
                                       (juv-sum (juv) (apply + juv))))
#[#{dataframe dqmpcr7n11a0dimehuppd0gd9-3}
  ((grp a b)
   (adult-sum 3 12)
   (juv-sum 30 120))
  (grp adult-sum juv-sum) (2 . 3)]

> (dataframe-aggregate df
                       '(grp trt)
                       (aggregate-expr (adult-sum (adult) (apply + adult))
                                       (juv-sum (juv) (apply + juv))))
#[#{dataframe dqmpcr7n11a0dimehuppd0gd9-3}
  ((grp a a b b)
   (trt a b a b)
   (adult-sum 1 2 3 9)
   (juv-sum 10 20 30 90))
  (grp trt adult-sum juv-sum) (4 . 4)]
```

## [Thread first and thread last](https://lispdreams.wordpress.com/2016/04/10/thread-first-thread-last-and-partials-oh-my/)  

#### <a name="thread-first"></a> procedure: `(-> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *first* argument of the next `expr`  

#### <a name="thread-last"></a> procedure: `(->> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *last* argument of the next `expr`  

```
> (define (mean ls) (/ (apply + ls) (length ls)))

> (-> '(1 2 3) (mean) (+ 10))
12

> (define x (-> '(1 2 3) (->> (apply +))))
> x
6

> (-> '((grp a a b b b)
        (trt a b a b b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (make-dataframe)
      (dataframe-modify
       (modify-expr (total (adult juv) (+ adult juv)))))
#[#{dataframe j3vvfoehucee2musfnx4eje5e-4}
  ((grp a a b b b)
   (trt a b a b b)
   (adult 1 2 3 4 5)
   (juv 10 20 30 40 50)
   (total 11 22 33 44 55))
  (grp trt adult juv total) (5 . 5)]
  
> (-> '((grp a a b b b)
        (trt a b a b b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (make-dataframe)
      (dataframe-split 'grp)
      (->> (map (lambda (df)
                  (dataframe-modify
                   df
                   (modify-expr (juv-mean () (mean ($ df 'juv))))))))
      (->> (apply dataframe-bind))
      (dataframe-filter (filter-expr (juv juv-mean) (> juv juv-mean))))
#[#{dataframe j3vvfoehucee2musfnx4eje5e-4}
  ((grp a b)
   (trt b b)
   (adult 2 5)
   (juv 20 50)
   (juv-mean 15 40))
  (grp trt adult juv juv-mean) (2 . 5)]
```


