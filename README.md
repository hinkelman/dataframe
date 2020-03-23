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

[`(make-dataframe alist)`](#procedure-make-dataframe-alist)  
[`(dataframe-equal? . dfs)`](#procedure-dataframe-equal-dfs)  
[`(dataframe-head df n)`](#procedure-dataframe-head-df-n)  
[`(dataframe-tail df n)`](#procedure-dataframe-tail-df-n)  
[`(dataframe-values df name)`](#procedure-dataframe-values-df-name)  

### Select, drop, and rename columns  

[`(dataframe-select df . names)`](#procedure-dataframe-select-df-names)  
[`(dataframe-drop df . names)`](#procedure-dataframe-drop-df-names)  

### Filter and sort  

### Append and split  

### Modify and aggregate  

## Dataframe record type  

#### procedure: `(make-dataframe alist)`
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
```

#### procedure: `(dataframe-equal? . dfs)`
**returns:** `#t` if all dataframes are equal, `#f` otherwise  

```
> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((b 4 5 6) (a 1 2 3))))
#f
> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((a 10 2 3) (b 4 5 6))))
#f
```

#### procedure: `(dataframe-values df name)`
**returns:** list of values for column `name` in `df`  

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

#### procedure: `(dataframe-select df . names)`
**returns:** dataframe of columns with `names` selected from `df`   

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-select df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]
> (dataframe-select df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((c 7 8 9) (b 4 5 6)) (c b) (3 . 2)]
```

#### procedure: `(dataframe-select df . names)`
**returns:** dataframe of columns with `names` dropped from `df`   

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
