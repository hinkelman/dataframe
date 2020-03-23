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
[`(dataframe-equal? . dfs)`](#procedure-dataframe-equal?-.-dfs)  

### Select, drop, and rename columns  

### Filter and sort  

### Append and split  

### Modify and aggregate  

## Dataframe record type  

#### procedure: `(make-dataframe alist)`
**returns:** a dataframe record type with three fields: alist, names, and dim  

```
> (define df1 (make-dataframe '((a 1 2 3) (b 4 5 6))))
> df1
#[#{dataframe cziqfonusl4ihl0gdwa8clop7-3} ((a 1 2 3) (b 4 5 6)) (a b) (3 . 2)]
> (dataframe? df1)
#t
> (dataframe? '((a 1 2 3) (b 4 5 6)))
#f
> (dataframe-alist df1)
((a 1 2 3) (b 4 5 6))
> (dataframe-names df1)
(a b)
> (dataframe-dim df1)
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

## Select, drop, and rename columns  

## Filter and sort  

## Append and split  

## Modify and aggregate  
