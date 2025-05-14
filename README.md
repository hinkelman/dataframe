# Scheme (R6RS) Dataframe Library

A dataframe record type with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, join, reshape, and aggregate dataframes. 

Related blog posts:  
[A dataframe record type for Scheme](https://www.travishinkelman.com/posts/dataframe-record-type-for-scheme/)  
[Select, drop, and rename dataframe columns in Scheme](https://www.travishinkelman.com/posts/select-drop-rename-dataframe-columns-scheme/)  
[Split, bind, and append dataframes in Scheme](https://www.travishinkelman.com/posts/split-bind-append-dataframes-scheme/)  
[Filter, partition, and sort dataframes in Scheme](https://www.travishinkelman.com/posts/filter-partition-and-sort-dataframes-in-scheme/)  
[Modify and aggregate dataframes in Scheme](https://www.travishinkelman.com/posts/modify-aggregate-dataframes-scheme/)  

## Installation

### Akku

```
$ akku install dataframe
```

For more information on getting started with [Akku](https://akkuscm.org/), see this [blog post](https://www.travishinkelman.com/posts/getting-started-with-akku-package-manager-for-scheme/).

## Import

`(import (dataframe))`

## Table of Contents  

### Type conversion  

[`(get-type obj)`](#get-type)  
[`(guess-type lst n-max)`](#guess-type)   
[`(convert-type obj type)`](#convert-type)  

### Series record type  

[`(make-series name lst)`](#make-series)  
[`(make-series* expr)`](#make-series*)  
[`(series? series)`](#series)  
[`(series-name series)`](#series-name)  
[`(series-lst series)`](#series-lst)  
[`(series-length series)`](#series-length)  
[`(series-type series)`](#series-type)  
[`(series-equal? series1 series2 ...)`](#series-equal)  

### Dataframe record type  

[`(make-dataframe slist)`](#make-df)  
[`(make-df* expr)`](#make-df*)  
[`(dataframe-slist df)`](#dataframe-slist)  
[`(dataframe-names df)`](#dataframe-names)  
[`(dataframe-dim df)`](#dataframe-dim)  
[`(dataframe-contains? df name ...)`](#df-contains)  
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe-ref df indices [name ...])`](#df-ref)  
[`(dataframe-series df name)`](#df-series)  
[`(dataframe-values df name)`](#df-values)  

### Dataframe display  

[`(dataframe-display df [n total-width min-width])`](#df-display)  
[`(dataframe-glimpse df [total-width])`](#df-glimpse)  

### Dataframe read/write  

[`(dataframe-write df path [overwrite])`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe->csv df path [overwrite])`](#df-csv)  
[`(dataframe->tsv df path [overwrite])`](#df-tsv)  
[`(csv->dataframe path [header])`](#csv-df)  
[`(tsv->dataframe path [header])`](#tsv-df)  

### Select, drop, and rename columns  

[`(dataframe-select df names)`](#df-select)  
[`(dataframe-select* df name ...)`](#df-select*)  
[`(dataframe-drop df names)`](#df-drop)  
[`(dataframe-drop* df name ...)`](#df-drop*)  
[`(dataframe-rename df old-names new-names)`](#df-rename)  
[`(dataframe-rename* df (old-name new-name) ...)`](#df-rename*)  
[`(dataframe-rename-all df new-names)`](#df-rename-all)  

### Filter

[`(dataframe-unique df)`](#df-unique)  
[`(dataframe-filter df names procedure)`](#df-filter)  
[`(dataframe-filter* df names expr)`](#df-filter*)  
[`(dataframe-filter-at df predicate name ...)`](#df-filter-at)  
[`(dataframe-filter-all df predicate)`](#df-filter-all)  
[`(dataframe-partition df names procedure)`](#df-partition)  
[`(dataframe-partition* df names expr)`](#df-partition*)  

### Sort 

[`(dataframe-sort df predicates names)`](#df-sort)  
[`(dataframe-sort* df (predicate name) ...)`](#df-sort*)  

### Split, bind, and append  

[`(dataframe-split df group-name ...)`](#df-split)  
[`(dataframe-bind df1 df2 [fill-value])`](#df-bind)  
[`(dataframe-bind-all dfs [fill-value])`](#df-bind-all)  
[`(dataframe-append df1 df2 ...)`](#df-append)

### Crossing  

[`(dataframe-crossing obj1 obj2 ...)`](#df-crossing)  

### Join  

[`(dataframe-inner-join df1 df2 join-names)`](#df-inner-join)  
[`(dataframe-left-join df1 df2 join-names [fill-value])`](#df-left-join)  
[`(dataframe-left-join-all dfs join-names [fill-value])`](#df-left-join-all)

### Reshape

[`(dataframe-stack df names names-to values-to)`](#df-stack)  
[`(dataframe-spread df names-from values-from [fill-value])`](#df-spread)  

### Modify and aggregate  

[`(dataframe-modify df new-names names procedure ...)`](#df-modify)  
[`(dataframe-modify* df (new-name names expr) ...)`](#df-modify*)  
[`(dataframe-modify-at df procedure name ...)`](#df-modify-at)  
[`(dataframe-modify-all df procedure)`](#df-modify-all)  
[`(dataframe-aggregate df group-names new-names names procedure ...)`](#df-aggregate)  
[`(dataframe-aggregate* df group-names (new-name names expr) ...)`](#df-aggregate*)  

### Thread first and thread last  

[`(-> expr ...)`](#thread-first)  
[`(->> expr ...)`](#thread-last)  

### Missing values  

[`(na? obj)`](#na)   
[`(any-na? lst)`](#any-na)   
[`(remove-na lst)`](#remove-na)  
[`(dataframe-remove-na df [name ...])`](#df-remove-na)  

### Descriptive statistics

[`(count obj lst)`](#count)  
[`(count-elements lst)`](#count-elements)  
[`(rle lst)`](#rle)  
[`(remove-duplicates lst)`](#remove-duplicates)  
[`(rep lst n type)`](#rep)  
[`(tranpose lst)`](#transpose)  
[`(sum lst [na-rm])`](#sum)  
[`(product lst [na-rm])`](#prod)  
[`(mean lst [na-rm])`](#mean)  
[`(weighted-mean lst weights [na-rm])`](#weighted-mean)  
[`(variance lst [na-rm])`](#variance)  
[`(standard-deviation lst [na-rm])`](#standard-deviation)  
[`(median lst [type na-rm])`](#median)  
[`(quantile lst p [type na-rm])`](#quantile)  
[`(interquartile-range lst [type na-rm])`](#iqr)  
[`(cumulative-sum lst)`](#cumulative-sum)  

## Type conversion  

#### <a name="get-type"></a> procedure: `(get-type obj)`  
**returns:** type of obj (bool, chr, str, sym, num, or other); strings that are valid numbers are assumed to be `'num`

#### <a name="guess-type"></a> procedure: `(guess-type lst n-max)`  
**returns:** type of elements in `lst` (bool, chr, str, sym, num, or other); evaluates up to `n-max` elements of `lst` before guessing; strings that are valid numbers are assumed to be `'num`

```
> (get-type "3")
num

> (get-type '(1 2 3))
other

> (guess-type '(1 2 3) 3)
num

> (guess-type '(1 "2" 3) 3)
num

> (guess-type '(a b c) 3)
sym

> (guess-type '(a b "c") 3)
str

> (guess-type '(a b "c") 2)
sym
```

#### <a name="convert-type"></a> procedure: `(convert-type obj type)`  
**returns:** an `obj` converted to `type`; elements that can't be converted to `type` are replaced with `'na`

```
;; arguably, this is overly opinionated, but was chosen to avoid surprise about things like 
;; (string->symbol "10") --> \x31;0
> (convert-type "c" 'sym)
na

> (convert-type 'b 'str)
"b"

> (map (lambda (x) (convert-type x 'other)) '(a b "c"))
(a b "c")

> (convert-type "3" 'num)
3

> (map (lambda (x) (convert-type x 'num)) '(1 2 3 na "" " " "NA" "na"))
(1 2 3 na na na na na)

> (map (lambda (x) (convert-type x 'str)) '(a "b" c na "" " " "NA" "na"))
("a" "b" "c" na na na na na)
```

## Series record type  

#### <a name="make-series"></a> procedure: `(make-series name lst)`  
**returns:** a series record type from `name` and `lst` with four fields: name, lst, length, and type  

#### <a name="make-series*"></a> procedure: `(make-series* expr)`  
**returns:** a series record type from `expr` with four fields: name, lst, length, and type  

```
> (make-series 'a '(1 2 3))
#[#{series oti45h148lm5x6fghpw1qhjz-20} a (1 2 3) (1 2 3) num 3]

> (make-series* (a 1 2 3))
#[#{series oti45h148lm5x6fghpw1qhjz-20} a (1 2 3) (1 2 3) num 3]

> (make-series 'a '(a b c))
#[#{series oti45h148lm5x6fghpw1qhjz-20} a (a b c) (a b c) sym 3]

> (make-series* (a 'a 'b 'c))
#[#{series oti45h148lm5x6fghpw1qhjz-20} a (a b c) (a b c) sym 3]
```

#### <a name="series"></a> procedure: `(series? series)`  
**returns:** `#t` if `series` is a series, `#f` otherwise  

#### <a name="series-name"></a> procedure: `(series-name series)`  
**returns:** `series` name  

#### <a name="series-lst"></a> procedure: `(series-lst series)`  
**returns:** `series` list  

#### <a name="series-length"></a> procedure: `(series-length series)`  
**returns:** `series` length  

```
> (define s (make-series 'a (iota 10)))

> (series-name s)
a

> (series-length s)
10

> (series-lst s)
(0 1 2 3 4 5 6 7 8 9)
```

#### <a name="series-type"></a> procedure: `(series-type series)`  
**returns:** `series` type (bool, chr, str, sym, num, or other); implicit conversion rules are applied in `make-series*`  

```
> (series-type (make-series* (a 1 2 3)))
num

> (series-type (make-series* (a 1 "2" 3)))
num

> (series-type (make-series* (a 1 "b" 3)))
str

> (series-type (make-series* (a "a" "b" "c")))
str

> (series-type (make-series* (a 'a 'b 'c)))
sym

> (series-type (make-series* (a 'a 'b "c")))
str

> (series-type (make-series* (a #t #f)))
bool

> (series-type (make-series* (a #t "#f")))
str

> (series-type (make-series* (a #\a #\b #\c)))
chr

> (series-type (make-series* (a #\a #\b "c")))
str

> (series-type (make-series* (a 1 2 '(3 4))))
other
```

#### <a name="series-equal"></a> procedure: `(series-equal? series1 series2 ...)`  
**returns:** `#t` if all `series` are equal, `#f` otherwise  

```
> (series-equal? 
    (make-series* (a 1 2 3))
    (make-series* (a 1 "2" 3)))
#t

> (series-equal? 
    (make-series* (a "a" "b" "c"))
    (make-series* (a 'a 'b "c")))
#t

> (series-equal? 
    (make-series* (a "a" "b" "c"))
    (make-series* (a 'a 'b 'c)))
#f

> (series-equal? 
    (make-series* (a 1 2 3))
    (make-series* (a 1 "2" 3))
    (make-series* (b 1 2 3)))
#f
```

## Dataframe record type  

#### <a name="make-df"></a> procedure: `(make-dataframe slist)`  
**returns:** a dataframe record type from a list of series (`slist`) with three fields: slist, names, and dim 

#### <a name="make-df*"></a> procedure: `(make-df* expr)`  
**returns:** a dataframe record type from `expr` with three fields: slist, names, and dim  

```
> (make-dataframe (list (make-series* (a 1 2 3)) (make-series* (b 4 5 6))))

#[#{dataframe mcq0csmab1sjwlyjv093af7t1-20} (#[#{series mcq0csmab1sjwlyjv093af7t1-21} a (1 2 3) (1 2 3) num 3] #[#{series mcq0csmab1sjwlyjv093af7t1-21} b (4 5 6) (4 5 6) num 3]) (a b) (3 . 2)]

> (make-df* (a 1 2 3) (b 4 5 6))

#[#{dataframe mcq0csmab1sjwlyjv093af7t1-20} (#[#{series mcq0csmab1sjwlyjv093af7t1-21} a (1 2 3) (1 2 3) num 3] #[#{series mcq0csmab1sjwlyjv093af7t1-21} b (4 5 6) (4 5 6) num 3]) (a b) (3 . 2)]

> (dataframe? (make-df* (a 1 2 3)))
#t

> (dataframe? (list (make-series* (a 1 2 3))))
#f

> (make-df* ("a" 1 2 3))
Exception in (make-series name src): name(s) not symbol(s)
```

#### <a name="dataframe-slist"></a> procedure: `(dataframe-slist df)`  
**returns:** a list of the series that comprise dataframe `df` 

```
> (dataframe-slist (make-df* (a 1 2 3) (b 4 5 6)))
(#[#{series cr52mzjx42dc7eg7ul2sn36zu-20} a (1 2 3) (1 2 3) num 3]
  #[#{series cr52mzjx42dc7eg7ul2sn36zu-20} b (4 5 6) (4 5 6) num 3])
```

#### <a name="dataframe-names"></a> procedure: `(dataframe-names df)`  
**returns:** a list of symbols representing the names of columns in dataframe `df` 

```
> (dataframe-names (make-df* (a 1) (b 2) (c 3) (d 4)))
(a b c d)
```

#### <a name="dataframe-dim"></a> procedure: `(dataframe-dim df)`  
**returns:** a pair of the number of rows and columns `(rows . columns)` in dataframe `df` 

```
> (dataframe-dim (make-df* (a 1) (b 2) (c 3) (d 4)))
(1 . 4)

> (dataframe-dim (make-df* (a 1 2 3) (b 4 5 6)))
(3 . 2)
```

#### <a name="df-contains"></a> procedure: `(dataframe-contains? df name ...)`  
**returns:** `#t` if all column `names` are found in dataframe `df`, `#f` otherwise  

```
> (define df (make-df* (a 1) (b 2) (c 3) (d 4)))

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
> (define df (make-df* (a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999)))

> (dataframe-display (dataframe-head df 3))

 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      1.      4.      7. 
      2.      5.      8. 
      3.      6.      9. 

> (dataframe-display (dataframe-tail df 2))

 dim: 4 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      3.      6.      9. 
      1.      4.   -999. 
      2.      5.   -999. 
      3.      6.   -999. 
```

#### <a name="df-equal"></a> procedure: `(dataframe-equal? df1 df2 ...)`  
**returns:** `#t` if all dataframes are equal, `#f` otherwise  

```
> (dataframe-equal? (make-df* (a 1 2 3))
                    (make-df* (a 1 "2" 3)))
#t

> (dataframe-equal? (make-df* (a 1 2 3) (b 4 5 6))
                    (make-df* (b 4 5 6) (a 1 2 3)))
#f

> (dataframe-equal? (make-df* (a 1 2 3) (b 4 5 6))
                    (make-df* (a 10 2 3) (b 4 5 6)))
#f
```

#### <a name="df-ref"></a> procedure: `(dataframe-ref df indices [name ...])`  
**returns:** a dataframe with only rows indicated by `indices` from dataframe `df`; default is to return all columns, but can optionally specify column `name(s)`  

```
> (define df (make-df* (a 100 200 300) (b 4 5 6) (c 700 800 900)))

> (dataframe-display df)
 
 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
    100.      4.    700. 
    200.      5.    800. 
    300.      6.    900. 

> (dataframe-display (dataframe-ref df '(0 2)))
 
 dim: 2 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
    100.      4.    700. 
    300.      6.    900. 

> (dataframe-display (dataframe-ref df '(0 2) 'a 'c))
 
 dim: 2 rows x 2 cols
       a       c 
   <num>   <num> 
    100.    700. 
    300.    900. 

```

#### <a name="df-series"></a> procedure: `(dataframe-series df name)`  
**returns:** a series for column `name` from dataframe `df`  

#### <a name="df-values"></a> procedure: `(dataframe-values df name)`  
**returns:** a list of values for column `name` from dataframe `df`  

```
> (define df (make-df* (a 100 200 300) (b 4 5 6) (c 700 800 900)))

> (dataframe-series df 'b)
#[#{series ey38a8jsdkhs5t8j9gl1fo67w-59} b (4 5 6) (4 5 6) num 3]

> (dataframe-values df 'b)
(4 5 6)

> ($ df 'b)                   ; $ is shorthand for dataframe-values; inspired by R, e.g., df$b.
(4 5 6)

> (map (lambda (name) ($ df name)) '(c a))
((700 800 900) (100 200 300))
```

## Dataframe display   

#### <a name="df-display"></a> procedure: `(dataframe-display df [n total-width min-width])`  
**displays:** the dataframe `df` up to `n` rows and the number of columns that fit in `total-width` based on the actual contents of column or minimum column width `min-width`; `total-width` and `min-width` are measured in number of characters; default values: `n = 10`, `total-width = 76`, `min-width = 7`  

#### <a name="df-glimpse"></a> procedure: `(dataframe-glimpse df [total-width])`  
**displays:** a transposed version of `dataframe-display` where the column names and types are displayed vertically and the data runs across the page up to `total-width`, which has a default value of 76. 

```
> (define df 
    (make-df* 
      (Boolean #t #f #t)
      (Char #\y #\e #\s)
      (String "these" "are" "strings")
      (Symbol 'these 'are 'symbols)
      (Exact 1/2 1/3 1/4)
      (Integer 1 -2 3)
      (Expt 1e6 -123456 1.2346e-6)
      (Dec4 132.1 -157 10.234)   ; based on size of numbers
      (Dec2 1234 5784 -76833.123)
      (Other (cons 1 2) '(a b c) (make-df* (a 2)))))
                          
> (dataframe-display df 3 90)

 dim: 3 rows x 10 cols
  Boolean    Char   String   Symbol   Exact  Integer       Expt       Dec4       Dec2        Other 
   <bool>   <chr>    <str>    <sym>   <num>    <num>      <num>      <num>      <num>      <other> 
       #t       y    these    these     1/2       1.   1.000E+6   132.1000    1234.00       <pair> 
       #f       e      are      are     1/3      -2.  -1.235E+5  -157.0000    5784.00       <list> 
       #t       s  strings  symbols     1/4       3.   1.235E-6    10.2340  -76833.12  <dataframe> 

> (dataframe-glimpse df)

 dim: 3 rows x 10 cols
 Boolean  <bool>   #t, #f, #t                                                   
 Char     <chr>    y, e, s                                                      
 String   <str>    these, are, strings                                          
 Symbol   <sym>    these, are, symbols                                          
 Exact    <num>    1/2, 1/3, 1/4                                                
 Integer  <num>    1, -2, 3                                                     
 Expt     <num>    1000000.0, -123456, 1.2346e-6                                
 Dec4     <num>    132.1, -157, 10.234                                          
 Dec2     <num>    1234, 5784, -76833.123                                       
 Other    <other>  <pair>, <list>, <dataframe>    
        
> (define df2 
    (make-dataframe
     (list 
      (make-series 'a (iota 25))
      (make-series 'b (map add1 (iota 25))))))

> (dataframe-display df2 5)

 dim: 15 rows x 2 cols
       a       b 
   <num>   <num> 
      0.      1. 
      1.      2. 
      2.      3. 
      3.      4. 
      4.      5. 

> (dataframe-glimpse df2)

 dim: 25 rows x 2 cols
 a       <num>   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, ...  
 b       <num>   1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, ... 
```

## Dataframe read/write    

#### <a name="df-write"></a> procedure: `(dataframe-write df path [overwrite])`  
#### <a name="df-csv"></a> procedure: `(dataframe->csv df path [overwrite])`  
#### <a name="df-tsv"></a> procedure: `(dataframe->tsv df path [overwrite])`  
**writes:** a dataframe `df` as a Scheme object or CSV/TSV file to `path`; default value for `overwrite` is #t  

#### <a name="df-read"></a> procedure: `(dataframe-read path)`  
#### <a name="csv-df"></a> procedure: `(csv->dataframe path [header])`  
#### <a name="tsv-df"></a> procedure: `(tsv->dataframe path [header])`  
**returns:** a dataframe from Scheme object or CSV/TSV file at `path`; for CSV/TSV file, default value for `header` is #t

```
> (define df 
    (make-df* 
      (Boolean #t #f #t)
      (Char #\y #\e #\s)
      (String "these" "are" "strings")
      (Symbol 'these 'are 'symbols)
      (Number 1.1 2 3.2)
      (Other (cons 1 2) '(a b c) (make-df* (a 2)))))

> (dataframe-display df)

 dim: 3 rows x 6 cols
  Boolean    Char   String   Symbol  Number        Other 
   <bool>   <chr>    <str>    <sym>   <num>      <other> 
       #t       y    these    these  1.1000       <pair> 
       #f       e      are      are  2.0000       <list> 
       #t       s  strings  symbols  3.2000  <dataframe> 

> (dataframe-write df "df-example.scm")

> (dataframe-display (dataframe-read "df-example.scm"))
 ;; types are preserved
 dim: 3 rows x 6 cols
  Boolean    Char   String   Symbol  Number        Other 
   <bool>   <chr>    <str>    <sym>   <num>      <other> 
       #t       y    these    these  1.1000       <pair> 
       #f       e      are      are  2.0000       <list> 
       #t       s  strings  symbols  3.2000  <dataframe> 

> (dataframe->csv df "df-example.csv")

> (dataframe-display (csv->dataframe "df-example.csv"))
 ;; types are not preserved; for `other`, values are not preserved
 dim: 3 rows x 6 cols
  Boolean    Char   String   Symbol  Number   Other 
    <str>   <str>    <str>    <str>   <num>    <na> 
       #t       y    these    these  1.1000      na 
       #f       e      are      are  2.0000      na 
       #t       s  strings  symbols  3.2000      na 
```

## Select, drop, and rename columns  

#### <a name="df-select"></a> procedure: `(dataframe-select df names)`  
**returns:** a dataframe of columns with `names` selected from dataframe `df`  

#### <a name="df-select*"></a> procedure: `(dataframe-select* df name ...)`  
**returns:** a dataframe of columns with `name(s)` selected from dataframe `df`  

```
> (define df (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9)))

> (dataframe-display (dataframe-select df '(a)))

 dim: 3 rows x 1 cols
       a 
   <num> 
      1. 
      2. 
      3. 

> (dataframe-display (dataframe-select* df a))

 dim: 3 rows x 1 cols
       a 
   <num> 
      1. 
      2. 
      3. 

> (dataframe-display (dataframe-select df '(c b)))

 dim: 3 rows x 2 cols
       c       b 
   <num>   <num> 
      7.      4. 
      8.      5. 
      9.      6. 

> (dataframe-display (dataframe-select* df c b))

 dim: 3 rows x 2 cols
       c       b 
   <num>   <num> 
      7.      4. 
      8.      5. 
      9.      6. 
```

#### <a name="df-drop"></a> procedure: `(dataframe-drop df name ...)`  
**returns:** a dataframe of columns with `names` dropped from dataframe `df`  

```
> (define df (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9)))

> (dataframe-display (dataframe-drop df '(c b)))

 dim: 3 rows x 1 cols
       a 
   <num> 
      1. 
      2. 
      3. 

> (dataframe-display (dataframe-drop* df c b))

 dim: 3 rows x 1 cols
       a 
   <num> 
      1. 
      2. 
      3. 

> (dataframe-display (dataframe-drop df '(a)))

 dim: 3 rows x 2 cols
       b       c 
   <num>   <num> 
      4.      7. 
      5.      8. 
      6.      9. 

> (dataframe-display (dataframe-drop* df a))

 dim: 3 rows x 2 cols
       b       c 
   <num>   <num> 
      4.      7. 
      5.      8. 
      6.      9. 

```

#### <a name="df-rename"></a> procedure: `(dataframe-rename df old-names new-names)`  
**returns:** a dataframe with a list of column names `old-names` from dataframe `df` renamed to `new-names`  

#### <a name="df-rename*"></a> procedure: `(dataframe-rename* df (old-name new-name) ...)`  
**returns:** a dataframe with column names from dataframe `df` renamed according to name pairs `(old-name new-name)`  

#### <a name="df-rename-all"></a> procedure: `(dataframe-rename-all df new-names)`  
**returns:** a dataframe with `new-names` replacing column names from dataframe `df`  

```
> (define df (make-df* (a 1 2 3) (b 4 5 6) (c 7 8 9)))

> (dataframe-display (dataframe-rename df '(b c) '(Bee Sea)))

 dim: 3 rows x 3 cols
       a     Bee     Sea 
   <num>   <num>   <num> 
      1.      4.      7. 
      2.      5.      8. 
      3.      6.      9. 

> (dataframe-display (dataframe-rename* df (b Bee) (c Sea)))

 dim: 3 rows x 3 cols
       a     Bee     Sea 
   <num>   <num>   <num> 
      1.      4.      7. 
      2.      5.      8. 
      3.      6.      9. 

;; no change made when old name is not found
> (dataframe-display (dataframe-rename* df (d Dee)))
 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      1.      4.      7. 
      2.      5.      8. 
      3.      6.      9. 

> (dataframe-display (dataframe-rename-all df '(A B C)))

 dim: 3 rows x 3 cols
       A       B       C 
   <num>   <num>   <num> 
      1.      4.      7. 
      2.      5.      8. 
      3.      6.      9. 
```

## Filter and sort  

#### <a name="df-unique"></a> procedure: `(dataframe-unique df)`  
**returns:** a dataframe with only the unique rows of dataframe `df`  

```
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

> (define df2 
    (make-df* 
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display
    (dataframe-unique (dataframe-select* df2 grp trt)))

 dim: 4 rows x 2 cols
     grp     trt 
   <sym>   <sym> 
       a       a 
       a       b 
       b       a 
       b       b 

```

#### <a name="df-filter"></a> procedure: `(dataframe-filter df names procedure)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to columns `names`

#### <a name="df-filter*"></a> procedure: `(dataframe-filter* df names expr)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `expr` applied to columns `names`

```
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
    (dataframe-filter df '(grp juv) (lambda (grp juv) (and (symbol=? grp 'b) (< juv 50)))))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       b       a      3.     30. 
       b       b      4.     40. 

> (dataframe-display 
    (dataframe-filter* df (grp juv) (and (symbol=? grp 'b) (< juv 50))))

 dim: 2 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       b       a      3.     30. 
       b       b      4.     40. 
```

#### <a name="df-filter-at"></a> procedure: `(dataframe-filter-at df procedure name ...)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to columns `names`  

#### <a name="df-filter-all"></a> procedure: `(dataframe-filter-all df procedure)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to all columns  

```
> (define df 
    (make-df* 
      (a 1 'na 3)
      (b 'na 5 6)
      (c 7 'na 9)))

> (dataframe-display df)

 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
       1      na       7 
      na       5      na 
       3       6       9 
  
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

#### <a name="df-partition"></a> procedure: `(dataframe-partition df names procedure)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned based on `procedure` applied to columns `names`  

#### <a name="df-partition*"></a> procedure: `(dataframe-partition* df names expr)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned based on `expr` applied to columns `names`  

```
> (define df 
    (make-df* 
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

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

> (dataframe-equal? keep keep*)
#t
> (dataframe-equal? drop drop*)
#t
```

## Sort 

#### <a name="df-sort"></a> procedure: `(dataframe-sort df predicates names)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according a list of `predicate` procedures acting on a list of column `names` 

#### <a name="df-sort*"></a> procedure: `(dataframe-sort* df (predicate name) ...)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according to the `predicate name` pairings

```
> (define df 
    (make-df* 
      (grp "a" "a" "b" "b" "b")
      (trt "a" "b" "a" "b" "b")
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

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

## Split, bind, and append  

#### <a name="df-split"></a> procedure: `(dataframe-split df group-names ...)`  
**returns:** list of dataframes split into unique groups by `group-names` from dataframe `df`; requires that all values in each grouping column are the same type  

```
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
  
> (for-each dataframe-display (dataframe-split df 'grp 'trt))

 dim: 1 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       a       a      1.     10. 

 dim: 1 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       a       b      2.     20. 

 dim: 1 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       b       a      3.     30. 

 dim: 2 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       b       b      4.     40. 
       b       b      5.     50. 
```

#### <a name="df-bind"></a> procedure: `(dataframe-bind df1 df2 [fill-value])`  
**returns:** a dataframe formed by binding all columns of the dataframes `df1` and `df2` where `fill-value` is used to fill values for columns that are not common to both dataframes; `fill-value` defaults to `'na'`

#### <a name="df-bind-all"></a> procedure: `(dataframe-bind-all dfs [fill-value])`  
**returns:** a dataframe formed by binding all columns of the list of dataframes `dfs`

```
> (define df 
    (make-df* 
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display (dataframe-bind-all (dataframe-split df 'grp 'trt)))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv 
   <sym>   <sym>   <num>   <num> 
       a       a      1.     10. 
       a       b      2.     20. 
       b       a      3.     30. 
       b       b      4.     40. 
       b       b      5.     50. 

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

> (dataframe-display (dataframe-bind df2 df1))

 dim: 6 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      4.     40.      na 
      5.     50.      na 
      6.     60.      na 
      1.     10.     100 
      2.     20.     200 
      3.     30.     300 

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
```

#### <a name="df-append"></a> procedure: `(dataframe-append df1 df2 ...)`  
**returns:** a dataframe formed by appending columns of the dataframes `df1 df2 ...`  

```
> (define df1 (make-df* (a 1 2 3) (b 4 5 6)))

> (define df2 (make-df* (c 7 8 9) (d 10 11 12)))

> (dataframe-display (dataframe-append df1 df2))

 dim: 3 rows x 4 cols
       a       b       c       d 
   <num>   <num>   <num>   <num> 
      1.      4.      7.     10. 
      2.      5.      8.     11. 
      3.      6.      9.     12. 
  
> (dataframe-display (dataframe-append df2 df1))

 dim: 3 rows x 4 cols
       c       d       a       b 
   <num>   <num>   <num>   <num> 
      7.     10.      1.      4. 
      8.     11.      2.      5. 
      9.     12.      3.      6. 
```

## Crossing  

#### <a name="df-crossing"></a> procedure: `(dataframe-crossing obj1 obj2 ...)`  
**returns:** a dataframe formed from the cartesian products of `obj1`, `obj2`, etc.; objects must be either series or dataframes

```
> (dataframe-display 
    (dataframe-crossing 
      (make-series* (col1 'a 'b))
      (make-series* (col2 'c 'd))))

 dim: 4 rows x 2 cols
    col1    col2 
   <sym>   <sym> 
       a       c 
       a       d 
       b       c 
       b       d 

> (dataframe-display 
      (dataframe-crossing 
        (make-series* (col1 'a 'b))
        (make-df* (col2 'c 'd))))

 dim: 4 rows x 2 cols
    col1    col2 
   <sym>   <sym> 
       a       c 
       a       d 
       b       c 
       b       d 

> (dataframe-display 
      (dataframe-crossing 
        (make-df* (col1 'a 'b) (col2 'c 'd))
        (make-df* (col3 'e 'f) (col4 'g 'h))))

 dim: 4 rows x 4 cols
    col1    col2    col3    col4 
   <sym>   <sym>   <sym>   <sym> 
       a       c       e       g 
       a       c       f       h 
       b       d       e       g 
       b       d       f       h 
```

## Join

#### <a name="df-inner-join"></a> procedure: `(dataframe-inner-join df1 df2 join-names)`  
**returns:** a dataframe formed by joining on the columns, `join-names`, of the dataframes `df1` and  `df2`; retains only rows that match in both dataframes

#### <a name="df-left-join"></a> procedure: `(dataframe-left-join df1 df2 join-names [fill-value])`  
**returns:** a dataframe formed by joining on the columns, `join-names`, of the dataframes `df1` and  `df2` where `df1` is the left dataframe; rows in `df1` not matched by any rows in `df2` are filled with `fill-value`, which defaults to `'na'`

#### <a name="df-left-join-all"></a> procedure: `(dataframe-left-join-all dfs join-names [fill-value])`  
**returns:** a dataframe formed by joining on the columns, `join-names`, of the list of dataframes `dfs` where each data frame is recursively joined to the previous one in the list

```
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

> (dataframe-display (dataframe-left-join df2 df1 '(site)))

 dim: 5 rows x 4 cols
    site     day   catch    habitat 
   <str>   <num>   <num>      <str> 
       c      1.     10.   woodland 
       c      2.     20.   woodland 
       b      1.     12.  grassland 
       b      2.     24.  grassland 
       d      1.    100.         na 

> (dataframe-display (dataframe-inner-join df2 df1 '(site)))

 dim: 4 rows x 4 cols
    site     day   catch    habitat 
   <str>   <num>   <num>      <str> 
       c      1.     10.   woodland 
       c      2.     20.   woodland 
       b      1.     12.  grassland 
       b      2.     24.  grassland 

> (dataframe-display (dataframe-left-join-all (list df2 df1) '(site)))

 dim: 5 rows x 4 cols
    site     day   catch    habitat 
   <str>   <num>   <num>      <str> 
       c      1.     10.   woodland 
       c      2.     20.   woodland 
       b      1.     12.  grassland 
       b      2.     24.  grassland 
       d      1.    100.         na 

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

> (dataframe-display (dataframe-left-join df3 df4 '(first last) -999))

 dim: 5 rows x 5 cols
   first    last     age    game   goals 
   <str>   <str>   <num>   <num>   <num> 
     sam     son     10.      1.      0. 
     bob     ert     20.      1.      1. 
     bob     ert     20.      2.      3. 
     sam     jam     30.   -999.   -999. 
     dan     man     40.      1.      2. 

> (dataframe-display (dataframe-inner-join df3 df4 '(first last)))

 dim: 4 rows x 5 cols
   first    last     age    game   goals 
   <str>   <str>   <num>   <num>   <num> 
     sam     son     10.      1.      0. 
     bob     ert     20.      1.      1. 
     bob     ert     20.      2.      3. 
     dan     man     40.      1.      2. 

> (dataframe-display (dataframe-left-join df4 df3 '(first last)))

 dim: 4 rows x 5 cols
   first    last    game   goals     age 
   <str>   <str>   <num>   <num>   <num> 
     sam     son      1.      0.     10. 
     bob     ert      1.      1.     20. 
     bob     ert      2.      3.     20. 
     dan     man      1.      2.     40. 
```

## Reshape

#### <a name="df-stack"></a> procedure: `(dataframe-stack df names names-to values-to)`  
**returns:** a dataframe formed by stacking pieces of a wide-format `df`; `names` is a list of column names to be combined into a single column; `names-to` is the name of the new column formed from the columns selected in `names`; `values-to` is the the name of the new column formed from the values in the columns selected in `names`

```
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

;; reshaping to long format is useful for aggregating
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

#### <a name="df-spread"></a> procedure: `(dataframe-spread df names-from values-from [fill-value])`  
**returns:** a dataframe formed by spreading a long format dataframe `df` into a wide-format dataframe; `names-from` is the name of the column containing the names of the new columns; `values-from` is the the name of the column containing the values that will be spread across the new columns; `fill-value` is used to fill combinations that are not found in the long format `df` and defaults to `'na`

```
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

## Modify and aggregate  

#### <a name="df-modify"></a> procedure: `(dataframe-modify df new-names names procedure ...)`  
**returns:** a dataframe where the columns `names` of dataframe `df` are modified according to the `procedure`  

#### <a name="df-modify*"></a> procedure: `(dataframe-modify* df (new-name names expr) ...)`  
**returns:** a dataframe where the columns `names` of dataframe `df` are modified according to the `expr`  

```
> (define df 
    (make-df* 
      (grp "a" "a" "b" "b" "b")
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))
                               
;; if new name occurs in dataframe, then column is replaced
;; if not, then new column is added

;; expr can refer to columns created in previous expr within the same call to dataframe-modify

;; if names is empty, 
;;   and procedure or expr is a scalar, then the scalar is repeated to match the number of rows in the dataframe
;;   and procedure or expr is a list of length equal to number of rows in dataframe, then the list is used as a column

> (dataframe-display
      (dataframe-modify
        df
        '(grp total prop-juv scalar lst)
        '((grp) (adult juv) (juv total) () ())
        (lambda (grp) (string-upcase grp))  
        (lambda (adult juv) (+ adult juv))
        (lambda (juv total) (/ juv total))
        (lambda () 42)
        (lambda () '(2 4 6 8 10))))

 dim: 5 rows x 8 cols
     grp     trt   adult     juv   total  prop-juv  scalar     lst 
   <str>   <sym>   <num>   <num>   <num>     <num>   <num>   <num> 
       A       a      1.     10.     11.     10/11     42.      2. 
       A       b      2.     20.     22.     10/11     42.      4. 
       B       a      3.     30.     33.     10/11     42.      6. 
       B       b      4.     40.     44.     10/11     42.      8. 
       B       b      5.     50.     55.     10/11     42.     10. 


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

#### <a name="df-modify-at"></a> procedure: `(dataframe-modify-at df procedure name ...)`  
**returns:** a dataframe where the specified columns `names` of dataframe `df` are modified based on `procedure`, which can only take one argument  

#### <a name="df-modify-all"></a> procedure: `(dataframe-modify-all df procedure)`  
**returns:** a dataframe where all columns of dataframe `df` are modified based on `procedure`, which can only take one argument  

```
> (define df 
    (make-df* 
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display (dataframe-modify-at df symbol->string 'grp 'trt))

 dim: 5 rows x 4 cols
     grp     trt   adult     juv 
   <str>   <str>   <num>   <num> 
       a       a      1.     10. 
       a       b      2.     20. 
       b       a      3.     30. 
       b       b      4.     40. 
       b       b      5.     50. 

> (define df2 
    (make-df* 
      (a 1 2 3)
      (b 4 5 6)
      (c 7 8 9)))

> (dataframe-display
    (dataframe-modify-all df2 (lambda (x) (* x 100))))

 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
    100.    400.    700. 
    200.    500.    800. 
    300.    600.    900. 
```

#### <a name="df-aggregate"></a> procedure: `(dataframe-aggregate df group-names new-names names procedure ...)`  
**returns:** a dataframe where the dataframe `df` is split according to list of `group-names` and aggregated according to the `procedure` applied to columns `names`

#### <a name="df-aggregate*"></a> procedure: `(dataframe-aggregate* df group-names (new-name names expr) ...)`  
**returns:** a dataframe where the dataframe `df` is split according to list of `group-names` and aggregated according to the `expr` applied to columns `names`  

```
> (define df 
    (make-df* 
      (grp 'a 'a 'b 'b 'b)
      (trt 'a 'b 'a 'b 'b)
      (adult 1 2 3 4 5)
      (juv 10 20 30 40 50)))

> (dataframe-display 
    (dataframe-aggregate 
      df
      '(grp)
      '(adult-sum juv-sum)
      '((adult) (juv))
      (lambda (adult) (sum adult))
      (lambda (juv) (sum juv)))) 

 dim: 2 rows x 3 cols
     grp  adult-sum  juv-sum 
   <sym>      <num>    <num> 
       a         3.      30. 
       b        12.     120. 

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

> (dataframe-display
    (dataframe-aggregate
      df
      '(grp trt)
      '(adult-sum juv-sum)
      '((adult) (juv))
      (lambda (adult) (sum adult))
      (lambda (juv) (sum juv))))

 dim: 4 rows x 4 cols
     grp     trt  adult-sum  juv-sum 
   <sym>   <sym>      <num>    <num> 
       a       a         1.      10. 
       a       b         2.      20. 
       b       a         3.      30. 
       b       b         9.      90. 

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

## Thread first and thread last

#### <a name="thread-first"></a> procedure: `(-> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *first* argument of the next `expr`  

#### <a name="thread-last"></a> procedure: `(->> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *last* argument of the next `expr`  

```
> (-> '(1 2 3) 
      (mean) 
      (+ 10))
12

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

## Missing values  
 
#### <a name="na"></a> procedure: `(na? obj)`  
**returns:** `#t` if `obj` is `'na` and `#f` otherwise

#### <a name="any-na"></a> procedure: `(any-na? lst)`  
**returns:** `#t` if any elements of `lst` are `'na` and `#f` otherwise

```
> (na? 'na)
#t
> (na? "na")
#f
> (na? 'NA)
#f
> (any-na? (iota 10))
#f
> (any-na? (cons 'na (iota 10)))
#t
> (any-na? (cons "na" (iota 10)))
#f
```

#### <a name="remove-na"></a> procedure: `(remove-na lst)`  
**returns:** a list with all `'na` elements removed from `lst`

```
> (remove-na '(1 na 2 3))
(1 2 3)
> (remove-na '(1 NA 2 3))
(1 NA 2 3)
> (remove-na '(1 "na" 2 3))
(1 "na" 2 3)
```

#### <a name="df-remove-na"></a> procedure: `(dataframe-remove-na df [name ...])`  
**returns:** a dataframe with any rows containing `'na` removed; by default, `'na` removed from all columns; optionally, can specify `name(s)` of columns from which to remove all `'na`

```
> (define df 
    (make-df* 
      (a 1 2 3 4 'na)
      (b 'na 7 8 9 10)
      (c 11 12 'na 14 15)))

> (dataframe-display (dataframe-remove-na df))
 dim: 2 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      2.      7.     12. 
      4.      9.     14. 

> (dataframe-display (dataframe-remove-na df 'a 'c))
 dim: 3 rows x 3 cols
       a       b       c 
   <num>   <num>   <num> 
      1.      na     11. 
      2.       7     12. 
      4.       9     14. 
```

## Descriptive statistics

#### <a name="count"></a> procedure: `(count obj lst)`
**returns:** number of `obj` in `lst`

#### <a name="count-elements"></a> procedure: `(count-elements lst)`
**returns:** list of pairs (element . count) for every unique element in `lst`

#### <a name="rle"></a> procedure: `(rle lst)`
**returns:** list of pairs (element . count) for the run-lenght encoding of `lst`

#### <a name="remove-duplicates"></a> procedure: `(remove-duplicates lst)`
**returns:** list of unique elements in `lst`

```
> (define x '(a b b c c c d d d d na))
> (count 'c x)
3
> (count 'e x)
0
> (count-elements x)
((a . 1) (b . 2) (c . 3) (d . 4) (na . 1))
> (rle x)
((a . 1) (b . 2) (c . 3) (d . 4) (na . 1))
> (rle '(1 1 2 1 1 0 2 2))
((1 . 2) (2 . 1) (1 . 2) (0 . 1) (2 . 2))
> (remove-duplicates x)
(a b c d na)
```
#### <a name="rep"></a> procedure: `(rep lst n type)`
**returns:** list formed by repeating `lst` `n` times; `type` should be either `'times` or `'each`

```
> (rep '(1 2) 3 'times)
(1 2 1 2 1 2)
> (rep '(1 2) 3 'each)
(1 1 1 2 2 2)
```

#### <a name="transpose"></a> procedure: `(transpose lst)`
**returns:** transposed list of elements in `lst`

```
> (transpose '((1 2 3 4) (5 6 7 8)))
((1 5) (2 6) (3 7) (4 8))
> (transpose '((1 5) (2 6) (3 7) (4 8)))
((1 2 3 4) (5 6 7 8))
```

#### <a name="sum"></a> procedure: `(sum lst [na-rm])`
**returns:** the sum of the values in `lst`; `na-rm` defaults to #t

```
> (sum (iota 10))
45
> (apply + (iota 10))
45
> (sum (cons 'na (iota 10)))
45
> (apply + (cons 'na (iota 10)))
Exception in +: na is not a number
> (sum (cons 'na (iota 10)) #f)
na
> (sum '(#t #f #t #f #t))
3
> (length (filter (lambda (x) x) '(#t #f #t #f #t)))
3

> (define df
    (make-df*
     (b 4 5 6)
     (c 7 8 'na)))

> (dataframe-display 
    (dataframe-modify* df5 (row-sum (a b c) (sum (list a b c)))))

 dim: 3 rows x 4 cols
       a       b       c  row-sum 
   <num>   <num>   <num>    <num> 
      1.      4.       7      12. 
      2.      5.       8      15. 
      3.      6.      na       9. 
```

#### <a name="prod"></a> procedure: `(product lst [na-rm])`
**returns:** the product of the values in `lst`; `na-rm` defaults to #t

```
> (product (map add1 (iota 10)))
3628800
> (apply * (map add1 (iota 10)))
3628800
> (product (cons 'na (map add1 (iota 10))))
> (product (cons 'na (map add1 (iota 10))) #f)
na
> (product '(#t #f #t #f #t))
0
```

#### <a name="mean"></a> procedure: `(mean lst [na-rm])`
**returns:** the arithmetic mean of the values in `lst`; `na-rm` defaults to #t

```
> (mean '(1 2 3 4 5))
3
> (mean '(-10 0 10))
0
> (mean '(-10 0 10 na) #f)
na
> (inexact (mean '(1 2 3 4 5 150)))
27.5
> (mean '(#t #f #t na))
2/3
```

#### <a name="weighted-mean"></a> procedure: `(weighted-mean lst weights [na-rm])`
**returns:** the arithmetic mean of the values in `lst` weighted by the values in `weights`; `na-rm` is only applied to `lst` and defaults to `#t`; any `'na` in weights yields `'na`

```
> (weighted-mean '(1 2 3 4 5) '(5 4 3 2 1))
7/3
> (weighted-mean '(1 2 3 4 na) '(5 4 3 2 1))
15/7
> (weighted-mean '(1 2 3 4 5) '(5 4 3 2 na))
na
> (weighted-mean '(1 2 3 4 5) '(2 2 2 2 2))
3
> (mean '(1 2 3 4 5))
3
> (weighted-mean '(1 2 3 4 5) '(2 0 2 2 2))
13/4
> (mean '(1 3 4 5))
13/4
```

#### <a name="variance"></a> procedure: `(variance lst [na-rm])`
**returns:** the sample variance of the values in `lst` based on [Welford's algorithm](https://www.johndcook.com/blog/standard_deviation/); `na-rm` defaults to `#t`

```
> (inexact (variance '(1 10 100 1000)))
233840.25
> (variance '(0 1 2 3 4 5))
7/2
```

#### <a name="standard-deviation"></a> procedure: `(standard-deviation lst [na-rm])`
**returns:** the standard deviation of the values in `lst`; `na-rm` defaults to `#t`

```
> (standard-deviation '(0 1 2 3 4 5))
1.8708286933869707
> (sqrt (variance '(0 1 2 3 4 5)))
1.8708286933869707
```

#### <a name="median"></a> procedure: `(median lst [type na-rm])`
**returns:** the median of `lst` corresponding to the given `type`, which defaults to 8 (see `quantile` for more info on `type`); `na-rm` defaults to `#t`

```
> (median '(1 2 3 4 5 6))
3.5
> (quantile '(1 2 3 4 5 6) 0.5)
3.5
```

#### <a name="quantile"></a> procedure: `(quantile lst p [type na-rm])`
**returns:** the sample quantile of the values in `lst` corresponding to the given probability, `p`, and `type`; `na-rm` defaults to #t

The quantile function follows [Hyndman and Fan 1996](https://www.jstor.org/stable/2684934) who recommend type 8, which is the default here. The [default in R](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html) is type 7.

```
> (quantile '(1 2 3 4 5 6) 0.5 1)
3
> (quantile '(1 2 3 4 5 6) 0.5 4)
3.0
> (quantile '(1 2 3 4 5 6) 0.5 8)
3.5
> (quantile '(1 2 3 4 5 6) 0.025 7)
1.125
```

#### <a name="iqr"></a> procedure: `(interquartile-range lst [type na-rm])`
**returns:** the difference in the 0.25 and 0.75 sample quantiles of the values in `lst` corresponding to the given `type`, which defaults to 8 (see `quantile` for more info on `type`); `na-rm` defaults to `#t`

```
> (interquartile-range '(1 2 3 5 5))
3.3333333333333335
> (interquartile-range '(1 2 3 5 5) 1)
3
> (interquartile-range '(3 7 4 8 9 7) 9)
4.125
```

#### <a name="cumulative-sum"></a> procedure: `(cumulative-sum lst)`
**returns:** a list that is the cumulative sum of the values in `lst`

```
> (cumulative-sum '(1 2 3 4 5))
(1 3 6 10 15)
> (cumulative-sum '(5 4 3 2 1))
(5 9 12 14 15)
> (cumulative-sum '(1 2 3 na 4))
(1 3 6 na na)
```