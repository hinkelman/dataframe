(library (dataframe df)
  (export
   ->
   ->>
   $
   aggregate-expr
   dataframe->rowtable
   dataframe?
   dataframe-aggregate
   dataframe-append
   dataframe-append-all
   dataframe-alist
   dataframe-contains?
   dataframe-dim
   dataframe-drop
   dataframe-equal?
   dataframe-filter
   dataframe-head
   dataframe-modify
   dataframe-names
   dataframe-names-update
   dataframe-partition
   dataframe-read
   dataframe-rename
   dataframe-select
   dataframe-sort
   dataframe-split
   dataframe-ref
   dataframe-tail
   dataframe-unique
   dataframe-values
   dataframe-write
   filter-expr
   make-dataframe
   modify-expr
   rowtable->dataframe
   sort-expr)

  (import (chezscheme)
          (dataframe assertions))

  ;; naming conventions ----------------------------------------------------------------------

  ;; dataframe: a record-type comprised of an association list that meets specified criteria
  ;; alist: list at the core of a dataframe; form is '((a 1 2 3) (b "this" "that" "other"))
  ;; alists: list of alists
  ;; column: one of the lists in the alist, includes name as first element
  ;; values: one of the lists in the alist, but with the name excluded
  ;; ls-values: values from a alist packaged into a list, e.g., '((1 2 3) ("this" "that" "other"))
  ;; name: column name
  ;; names: list of column names
  ;; group-names: list of column names used for grouping
  ;; df: single dataframe
  ;; dfs (or df-list): list of dataframes
  ;; name-pairs: alist used in dataframe-rename; form is '((old-name1 new-name1) (old-name2 new-name2))
  ;; ls: generic list
  ;; x: generic object
  ;; procedure:  procedure
  ;; procedures: list of procedures
  ;; bools: list of boolean values '(#t, #f)
  ;; row-based: describes orientation of list of lists; row-based example: '((a b) (1 "this") (2 "that") (3 "other))
  ;; rowtable: name used to describe a row-based list of lists where the first row represents column names
  ;; rt: single rowtable
  ;; ls-rows: generic row-based list of lists
  
  ;; thread-first and thread-last -------------------------------------------------------------
  
  ;; https://lispdreams.wordpress.com/2016/04/10/thread-first-thread-last-and-partials-oh-my/
  (define-syntax ->
    (syntax-rules ()
      [(_ value) value] 
      [(_ value (f1 . body) next ...)
       (-> (f1 value . body) next ...)]))

  (define (thread-last-helper f value . body)
    (apply f (append body (list value))))

  (define-syntax ->>
    (syntax-rules ()
      [(_ value) value]
      [(_ value (f1 . body) next ...)
       (->> (thread-last-helper f1 value . body) next ...)]))

  ;; dataframe record type ---------------------------------------------------------------------

  (define-record-type dataframe (fields alist names dim)
                      (protocol
                       (lambda (new)
                         (lambda (alist)
                           (let ([proc-string "(make-dataframe alist)"])
                             (check-alist alist proc-string))
                           (new alist
                                (map car alist)
                                (cons (length (cdar alist)) (length alist)))))))

  ;; check dataframes --------------------------------------------------------------------------
  
  (define (check-dataframe df who)
    (unless (dataframe? df)
      (assertion-violation who "df is not a dataframe")))

  (define (check-all-dataframes dfs who)
    (unless (for-all dataframe? dfs)
      (assertion-violation who "dfs are not all dataframes")))

  (define (dataframe-equal? . dfs)
    (check-all-dataframes dfs "(dataframe-equal? dfs)")
    (let* ([alists (map dataframe-alist dfs)]
           [first-alist (car alists)])
      (for-all (lambda (alist)
                 (equal? alist first-alist))
               alists)))

  ;; check dataframe attributes -------------------------------------------------------------
  
  (define (dataframe-contains? df . names)
    (check-dataframe df "(dataframe-contains? df names)")
    (let ([df-names (dataframe-names df)])
      (if (for-all (lambda (name) (member name df-names)) names) #t #f)))

  (define (check-names-exist df who . names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who "name(s) not in df")))

  (define (check-df-names df who . names)
    (check-dataframe df who)
    (check-names names who)
    (apply check-names-exist df who names))

  ;; dataframe-ref ------------------------------------------------------------------------------

  (define dataframe-ref
    (case-lambda
      [(df indices) (df-ref-helper df indices (dataframe-names df))]
      [(df indices . names)(df-ref-helper df indices names)]))

  (define (df-ref-helper df indices names)
    (let ([proc-string "(dataframe-ref df indices names)"]
          [n-max (car (dataframe-dim df))])
      (apply check-df-names df proc-string names)
      (check-list indices "indices" proc-string)
      (map (lambda (n)
             (check-integer-gte-zero n "index" proc-string)
             (check-index n n-max proc-string))
           indices))
    (make-dataframe (alist-ref (dataframe-alist df) indices names)))
    
  (define (alist-ref alist indices names)
    (let ([ls-values (alist-values-map alist names)])
      (add-names-ls-values
       names
       (map (lambda (values)
              (map (lambda (n) (list-ref values n)) indices))
            ls-values))))
    
  ;; head/tail -----------------------------------------------------------------------------------

  (define (dataframe-head df n)
    (dataframe-head-tail df n "head"))

  ;; dataframe-tail is based on list-tail, which does not work the same as tail in R
  (define (dataframe-tail df n)
    (dataframe-head-tail df n "tail"))

  (define (dataframe-head-tail df n type)
    (let ([proc-string (string-append "(dataframe-" type " df n)")]
          [check-integer (if (string=? type "head") check-integer-positive check-integer-gte-zero)]
          [proc (if (string=? type "head") list-head list-tail)])
      (check-dataframe df proc-string)
      (check-integer n "n" proc-string)
      (check-index n (car (dataframe-dim df)) proc-string)
      (make-dataframe
       (map (lambda (col) (cons (car col) (proc (cdr col) n)))
            (dataframe-alist df)))))
  
  ;; rename columns ---------------------------------------------------------------------------------

  ;; name-pairs is of form '((old-name1 new-name1) (old-name2 new-name2))
  (define (dataframe-rename df name-pairs)
    (let ([proc-string "(dataframe df name-pairs)"])
      (check-dataframe df proc-string)
      (check-name-pairs (dataframe-names df) name-pairs proc-string))
    (let ([alist (map (lambda (column)
                        (let* ([name (car column)]
                               [ls-values (cdr column)]
                               [name-match (assoc name name-pairs)])
                          (if name-match
                              (cons (cadr name-match) ls-values)
                              column)))
                      (dataframe-alist df))])
      (make-dataframe alist)))

  (define (dataframe-names-update df names)
    (let ([proc-string "(dataframe-names-update df names)"])
      (check-dataframe df proc-string)
      (check-names names proc-string)
      (let ([names-length (length names)]
            [num-cols (cdr (dataframe-dim df))])
        (unless (= names-length num-cols)
          (assertion-violation proc-string (string-append
                                            "names length must be "
                                            (number->string num-cols)
                                            ", not "
                                            (number->string names-length)))))
      (let* ([alist (dataframe-alist df)]
             [ls-values (map cdr alist)])
        (make-dataframe (add-names-ls-values names ls-values)))))

  ;; add names to list of values, ls-values, to create association list
  (define (add-names-ls-values names ls-values)
    (if (null? ls-values)
        (map (lambda (name) (cons name '())) names)
        (map (lambda (name vals) (cons name vals)) names ls-values)))
  
  ;; append -----------------------------------------------------------------------------------

  (define (dataframe-append . dfs)
    (let ([proc-string "(dataframe-append dfs)"])
      (check-all-dataframes dfs proc-string)
      (let ([names (apply shared-names dfs)])
        (when (null? names) (assertion-violation proc-string "no names in common across dfs"))
        (let ([alist (map (lambda (name)
                            ;; missing-value will not be used so chose arbitrary value (-999)
                            (cons name (apply append-columns name -999 dfs)))
                          names)])
          (make-dataframe alist)))))
  
  (define (shared-names . dfs)
    (let ([first-names (dataframe-names (car dfs))]
          [rest-names (apply all-unique-names (cdr dfs))])
      (filter (lambda (name) (member name rest-names)) first-names)))

  (define (dataframe-append-all missing-value . dfs)
    (check-all-dataframes dfs "(dataframe-append-all missing-value dfs)")
    (let* ([names (apply combine-names-ordered dfs)]
           [alist (map (lambda (name)
                         (cons name (apply append-columns name missing-value dfs)))
                       names)])
      (make-dataframe alist)))

  (define (append-columns name missing-value . dfs)
    (apply append
           (map (lambda (df)
                  (if (dataframe-contains? df name)
                      (dataframe-values df name)
                      (make-list (car (dataframe-dim df)) missing-value))) 
                dfs)))

  (define (all-names . dfs)
    (apply append (map (lambda (df) (dataframe-names df)) dfs)))

  (define (all-unique-names . dfs)
    (remove-duplicates (apply all-names dfs)))

  ;; combine names such that they stay in the order that they appear in each dataframe
  (define (combine-names-ordered . dfs)
    (define (loop all-names results)
      (cond [(null? all-names)
             (reverse results)]
            [(member (car all-names) results)
             (loop (cdr all-names) results)]
            [else
             (loop (cdr all-names) (cons (car all-names) results))]))
    (loop (apply all-names dfs) '()))
  
  ;; read/write ------------------------------------------------------------------------------

  (define (dataframe-write df path overwrite?)
    (when (and (file-exists? path) (not overwrite?))
      (assertion-violation path "file already exists"))
    (delete-file path)
    (with-output-to-file path
      (lambda () (write (dataframe-alist df)))))

  (define (dataframe-read path)
    (make-dataframe (with-input-from-file path read)))

  ;; extract values ------------------------------------------------------------------------------

  ;; returns simple list
  (define (dataframe-values df name)
    (let ([proc-string "(dataframe-values df name)"])
      (check-dataframe df proc-string)
      (check-names-exist df proc-string name))
    (alist-values (dataframe-alist df) name))

  (define ($ df name)
    (dataframe-values df name))

  (define (alist-values alist name)
    (cdr (assoc name alist)))

  (define (dataframe-values-map df names)
    (alist-values-map (dataframe-alist df) names))

  (define (alist-values-map alist names)
    (map (lambda (name) (alist-values alist name)) names))

  ;; select/drop columns ------------------------------------------------------------------------

  (define (dataframe-select df . names)
    (apply check-df-names df "(dataframe-select df names)" names)
    (make-dataframe (alist-select (dataframe-alist df) names)))

  (define (alist-select alist names)
    (map (lambda (name) (assoc name alist)) names))

  (define (dataframe-drop df . names)
    (apply check-df-names df "(dataframe-drop df names)" names)
    (make-dataframe (alist-drop (dataframe-alist df) names)))

  (define (alist-drop alist names)
    (filter (lambda (column) (not (member (car column) names))) alist))

  ;; filter/partition ------------------------------------------------------------------------

  (define (dataframe-partition df filter-expr)
    (let* ([bools (filter-map df filter-expr)]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)])
      (let-values ([(keep drop) (partition-ls-values bools (map cdr alist))])
        (values (make-dataframe (add-names-ls-values names keep))
                (make-dataframe (add-names-ls-values names drop))))))

  ;; partition list of values, ls-values, based on list of boolean values, bools
  ;; where each sub-list is same length as bools
  (define (partition-ls-values bools ls-values)
    (let loop ([bools bools]
               [ls-values ls-values]
               [keep '()]
               [drop '()])
      (if (null? bools)
          (values (map reverse keep)
                  (map reverse drop))
          (if (car bools)  ;; ls is list of boolean values
              (loop (cdr bools) (map cdr ls-values) (cons-acc ls-values keep) drop)
              (loop (cdr bools) (map cdr ls-values) keep (cons-acc ls-values drop))))))

  ;; cons list of values, ls-values, (usually length one) onto accumulator, acc
  (define (cons-acc ls-values acc)
    (if (null? acc)
        (map (lambda (x) (list (car x))) ls-values)
        (map (lambda (x y) (cons x y)) (map car ls-values) acc)))

  (define-syntax filter-expr
    (syntax-rules ()
      [(_ names expr)
       (list (quote names) (lambda names expr))]))

  (define (filter-map df filter-expr)
    (let ([names (car filter-expr)]
          [proc (cadr filter-expr)])
      (apply check-df-names df "(dataframe-filter df filter-expr)" names)
      (apply map proc (map (lambda (name) ($ df name)) names))))

  (define (dataframe-filter df filter-expr)
    (let* ([bools (filter-map df filter-expr)]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)]
           [new-ls-values (filter-ls-values bools (map cdr alist))])
      (make-dataframe (add-names-ls-values names new-ls-values))))
  
  ;; filter list of values, ls-values, based on list of boolean values, bools
  ;; where each sub-list is same length as bools
  ;; could just call (partition-ls-values) and return only the first value
  ;; but avoiding potential overhead of accumulating values that aren't used
  (define (filter-ls-values bools ls-values)
    (let loop ([bools bools]
               [ls-values ls-values]
               [results '()])
      (if (null? bools)
          (map reverse results)
          (if (car bools)
              (loop (cdr bools) (map cdr ls-values) (cons-acc ls-values results))
              (loop (cdr bools) (map cdr ls-values) results)))))

  ;; in some simple and now deleted tests
  ;; using built-in filter function is only slightly slower than recursive version
  ;; built-in version requires switching to row-wise and back to col-wise (based on my current scheme abilities)
  ;; i.e., it would be faster if dataframe use row-wise structure

  ;; select, mutate, aggregate are obviously better as column-wise
  ;; filter on a single column would be faster using row-wise
  ;; but want flexibility of filtering on multiple columns

  ;; sort ------------------------------------------------------------------------

  (define-syntax sort-expr
    (syntax-rules ()
      [(_ (predicate name) ...)
       (list
        (list predicate ...)
        (list (quote name) ...))]))

  (define (dataframe-sort df sort-expr)
    (let* ([predicates (car sort-expr)]
           [names (cadr sort-expr)]
           [alist (dataframe-alist df)]
           [all-names (map car alist)]
           [ranks (sum-col-ranks alist predicates names)]
           [ls-values-sorted (ls-values-sort > (cons ranks (map cdr alist)))])
      (make-dataframe (add-names-ls-values all-names (cdr ls-values-sorted)))))

  (define (ls-values-sort predicate ls-values)
    (transpose (sort-ls-row predicate (transpose ls-values))))

  (define (sort-ls-row predicate ls-row)
    (sort (lambda (x y) (predicate (car x)(car y))) ls-row))

  (define (sum-col-ranks alist predicates names)
    (let* ([ls-values (alist-values-map alist names)]
           [weights (map (lambda (x) (expt 10 x)) (enumerate names))]
           [ls-ranks (map (lambda (predicate ls weight)
                            (rank-list predicate ls weight))
                          predicates ls-values weights)])
      (apply map + ls-ranks)))
      
  (define (rank-list predicate ls weight)
    (let* ([unique-sorted (sort predicate (remove-duplicates ls))]
           [ranks (map (lambda (x) (/ x weight)) (enumerate unique-sorted))]
           [lookup (map (lambda (x y) (cons x y)) unique-sorted ranks)])
      (map (lambda (x) (cdr (assoc x lookup))) ls)))

  ;; (define (other-names selected-names all-names)
  ;;   (let* ([bools (map (lambda (x) (not (member x selected-names))) all-names)]
  ;;          [ls-rows (transpose (list bools all-names))])
  ;;     (map cadr (filter (lambda (x) (equal? #t (car x))) ls-rows))))

  ;; unique ------------------------------------------------------------------------

  (define (dataframe-unique df)
    (check-dataframe df "(dataframe-unique df)")
    (make-dataframe (alist-unique (dataframe-alist df))))
  
  (define (alist-unique alist)
    (let ([names (map car alist)]
          [ls-values (map cdr alist)])
      (add-names-ls-values names (ls-values-unique ls-values #f))))

  (define (ls-values-unique ls-values row-based)
    (let ([ls-rows (remove-duplicates (transpose ls-values))])
      (if row-based ls-rows (transpose ls-rows))))

  (define (transpose ls)
    (apply map list ls))

  ;; split ------------------------------------------------------------------------

  ;; returns boolean list of same length as ls
  ;; boolean list used to identify rows that are equal to focal value, x
  (define (map-equal? x ls)
    (let ([pred (cond
                 [(number? x) =]
                 [(string? x) string=?]
                 [(symbol? x) symbol=?]
                 [else equal?])])
      (map (lambda (y) (pred x y)) ls)))

  ;; andmap-equal? probably not a good name
  ;; objective is to identify rows from ls-values where every row matches target values in ls
  (define (andmap-equal? ls ls-values)
    (let* ([bools (map (lambda (x values)
                         (map-equal? x values))
                       ls
                       ls-values)]
           [ls-row (transpose bools)])
      (map (lambda (row)
             (for-all (lambda (x) (equal? x #t)) row))
           ls-row)))

  (define (alist-split-helper ls group-names alist)
    (let ([names (map car alist)]
          [ls-values (map cdr alist)]
          [bools (andmap-equal?
                  ls
                  (map cdr (alist-select alist group-names)))])
      (let-values ([(keep drop) (partition-ls-values bools ls-values)])
        (values (add-names-ls-values names keep)
                (add-names-ls-values names drop)))))

  ;; returns two values
  ;; first value is list of alists representing all columns in the dataframe
  ;; second value is a list of alists representing the grouping columns in the dataframe
  (define (alist-split alist group-names return-groups?)
    (define (loop ls-row-unique alist alists groups)
      (cond [(null? ls-row-unique)
             (values (reverse alists)
                     (reverse groups))]
            [else
             ;; group-values is asingle row of values representing one unique grouping combination
             (let ([group-values (car ls-row-unique)]) 
               (let-values ([(keep drop) (alist-split-helper group-values group-names alist)])
                 (loop (cdr ls-row-unique)
                       drop
                       (cons keep alists)
                       (cons (add-names-ls-values
                              group-names
                              (transpose (list group-values)))
                             groups))))])) 
    (let* ([ls-values-select (map cdr (alist-select alist group-names))]
           [ls-row-unique (ls-values-unique ls-values-select #t)])
      (let-values ([(alists groups) (loop ls-row-unique alist '() '())])
        (if return-groups?
            (values alists groups)
            alists))))

  (define (dataframe-split-helper df group-names return-groups?)
    (apply check-df-names df "(dataframe-split df group-names)" group-names)
    (let-values ([(alists groups) (alist-split (dataframe-alist df) group-names #t)])
      (let ([dfs (map make-dataframe alists)])
        (if return-groups?
            (values dfs groups)
            dfs))))

  (define (dataframe-split df . group-names)
    (dataframe-split-helper df group-names #f))

  ;; modify/add columns ------------------------------------------------------------------------

  ;; (dataframe-list-modify) doesn't work when making a "non-vectorized" calculation, e.g., (mean ($ df 'count), and, thus, doesn't seem that useful 
  ;; (define (dataframe-list-modify df-list modify-expr)
  ;;   (apply dataframe-append (map (lambda (df) (dataframe-modify df modify-expr)) df-list)))

  (define (dataframe-modify df modify-expr)
    (let* ([names (car modify-expr)]
           [ls-values (modify-map df (cadr modify-expr) (caddr modify-expr))]
           [alist (dataframe-alist df)])
      (check-names names "(dataframe-modify df modify-expr)")
      (make-dataframe (alist-modify-loop alist names ls-values))))

  (define (alist-modify alist name values)
    (let ([col (cons name values)]
          [all-names (map car alist)])
      (if (member name all-names)
          (map (lambda (x)
                 (if (symbol=? x name) col (assoc x alist)))
               all-names)                            
          (cons-end alist col))))

  (define (alist-modify-loop alist names ls-values)
    (if (null? names)
        alist
        (alist-modify-loop
         (alist-modify alist (car names) (car ls-values))
         (cdr names) (cdr ls-values))))
  
  (define (cons-end ls x)
    (reverse (cons x (reverse ls))))

  (define-syntax modify-expr
    (syntax-rules ()
      [(_ (new-name names expr) ...)
       (list
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))

  (define (modify-map df names-list proc-list)
    (let ([proc-string "(modify-expr new-name names expr)"])
      (map (lambda (names proc)
             (cond [(null? names)
                    (modify-map-helper df (proc) proc-string)]
                   [else
                    (apply check-df-names df proc-string names)
                    (apply map proc
                           (map (lambda (name) ($ df name)) names))]))
           names-list proc-list)))

  ;; this helper procedure returns values for a column from a scalar or list of same length as number of df rows
  (define (modify-map-helper df values who)
    (let ([df-length (car (dataframe-dim df))])
      (cond [(scalar? values)
             (make-list df-length values)]
            [(and (list? values) (= (length values) df-length))
             values]
            [else
             (assertion-violation
              who
              (string-append
               "value(s) must be scalar or list of length "
               (number->string df-length)))])))

  (define (scalar? obj)
    (or (symbol? obj) (char? obj) (string? obj) (number? obj)))

  ;; procedure like this seems useful  but the "API" is not consistent with dataframe-modify
  ;; (define (dataframe-modify-selected df procedure . names)
  ;;   (let ([proc-string "(dataframe-update df procedure names)"])
  ;;     (check-procedure procedure proc-string)
  ;;     (apply check-df-names df proc-string names))
  ;;   (let ([alist (map (lambda (column)
  ;;                       (if (member (car column) names)
  ;;                           (list (car column) (map procedure (cdr column)))
  ;;                           column))
  ;;                     (dataframe-alist df))])
  ;;     (make-dataframe alist)))
  
  ;; aggregate  ------------------------------------------------------------------------

  (define (dataframe-aggregate df group-names aggregate-expr)
    (let-values ([(df-list groups-list) (dataframe-split-helper df group-names #t)])
      (apply dataframe-append
             (map (lambda (df groups)
                    (df-aggregate-helper df groups aggregate-expr)) df-list groups-list))))
  
  (define (df-aggregate-helper df groups aggregate-expr)
    (let* ([names (car aggregate-expr)]
           [ls-values (aggregate-map df (cadr aggregate-expr) (caddr aggregate-expr))])
      (check-names names "(dataframe-aggregate df aggregate-expr)")
      (make-dataframe (alist-modify-loop groups names ls-values))))

  (define (aggregate-map df names-list proc-list)
    (map (lambda (names proc)
           (apply check-df-names df "(aggregate-expr new-name names expr)" names)
           (list (apply proc (map (lambda (name) ($ df name)) names))))
         names-list proc-list))

  ;; same macro as for modify-expr, but named to match the aggregate procedure
  (define-syntax aggregate-expr
    (syntax-rules ()
      [(_ (new-name names expr) ...)
       (list
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))

  ;; rowtable ------------------------------------------------------------------------

  ;; rowtable is a bad name to describe list of rows; as used in read-csv and write-csv in (chez-stats csv)

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-values (dataframe-values-map df names)])
      (cons names (transpose ls-values))))

  (define (rowtable->dataframe rt header?)
    (check-rowtable rt "(rowtable->dataframe rt header?)")
    (let ([names (if header?
                     (car rt)
                     (map string->symbol
                          (map string-append
                               (make-list (length (car rt)) "V")
                               (map number->string
                                    (enumerate (car rt))))))]
          [ls-values (if header?
                         (transpose (cdr rt))
                         (transpose rt))])
      (make-dataframe (map cons names ls-values))))

  )

