# Chez Scheme Dataframe Library

A dataframe record type for Chez Scheme with procedures to select, drop, and rename columns, and filter, sort, split, append, modify, and aggregate dataframes. 

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
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-write df path overwrite?)`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe->rowtable df)`](#df-rows)  
[`(rowtable->dataframe rt header?)`](#rows-df)  
[`(dataframe-values df name)`](#df-values)  

### Select, drop, and rename columns  

[`(dataframe-select df name ...)`](#df-select)  
[`(dataframe-drop df name ...)`](#df-drop)  

### Filter and sort  

### Append and split  

### Modify and aggregate  

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
(3 . 2)
> (define df (make-dataframe '(("a" 1 2 3) ("b" 4 5 6))))

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-head"></a> procedure: `(dataframe-head df n)`  
**returns:** a dataframe with first `n` rows from `df`  

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))
> (dataframe-head df 3)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]
```

#### <a name="df-tail"></a> procedure: `(dataframe-tail df n)`  
**returns:** a dataframe with the `n`th tail (zero-based) rows from `df`  

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))
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
**returns:** writes a dataframe as a Scheme object to `path`, if file exists at `path`, operation will fail unless `overwrite?` is #t  

```
> (define df (make-dataframe '((grp "b" "b" "a" "b" "a")
                               (trt b b b a a)
                               (adult 5 4 2 3 1)
                               (juv 50 40 20 30 10))))
> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((grp "b" "b" "a" "b" "a") (trt b b b a a) (adult 5 4 2 3 1) (juv 50 40 20 30 10)) (grp trt adult juv) (5 . 4)]
> (dataframe-write df "df-example.scm" #t)
```

#### <a name="df-read"></a> procedure: `(dataframe-read path)`  
**returns:** a dataframe read from `path`

```
;; example requires that you first run code for `dataframe-write`
> (define df2 (dataframe-read "df-example.scm"))
> df2
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((grp "b" "b" "a" "b" "a") (trt b b b a a) (adult 5 4 2 3 1) (juv 50 40 20 30 10)) (grp trt adult juv) (5 . 4)]
```

#### <a name="df-rows"></a> procedure: `(dataframe->rowtable df)`  
**returns:** a rowtable from `df`

```
;; a dataframe is a column-based data structure; a rowtable is a row-based data structure
> (define df (make-dataframe '((a 100 300) (b 4 6) (c 700 900))))
> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 100 300) (b 4 6) (c 700 900)) (a b c) (2 . 3)]
> (dataframe->rowtable df)
((a b c) (100 4 700) (300 6 900))
```

#### <a name="rows-df"></a> procedure: `(rowtable->dataframe rt header?)`  
**returns:** a dataframe from `rt`; if `header?` is `#f` a header row is created

```
;; a rowtable is a row-based data structure; a dataframe is a column-based data structure
> (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9)) #t)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]
> (rowtable->dataframe '((1 4 7) (2 5 8) (3 6 9)) #f)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((V0 1 2 3) (V1 4 5 6) (V2 7 8 9)) (V0 V1 V2) (3 . 3)]
> (rowtable->dataframe '(("a" "b" "c") (1 4 7) (2 5 8) (3 6 9)) #t)

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-values"></a> procedure: `(dataframe-values df name)`  
**returns:** list of values for column `name` from `df`  

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
**returns:** a dataframe of columns with `names` selected from `df`   

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-select df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]
> (dataframe-select df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((c 7 8 9) (b 4 5 6)) (c b) (3 . 2)]
```

#### <a name="df-drop"></a> procedure: `(dataframe-select df name ...)`  
**returns:** a dataframe of columns with `names` dropped from `df`   

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-drop df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]
> (dataframe-drop df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((b 4 5 6) (c 7 8 9)) (b c) (3 . 2)]
```

## Filter and sort  

## Append and split  

## Modify and aggregate  
