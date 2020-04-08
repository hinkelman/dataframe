(library (dataframe df)
  (export
   ->
   ->>
   $
   aggregate-expr
   dataframe->rowtable
   dataframe?
   dataframe-aggregate
   dataframe-alist
   dataframe-append
   dataframe-bind
   dataframe-bind-all
   dataframe-contains?
   dataframe-dim
   dataframe-drop
   dataframe-equal?
   dataframe-filter
   dataframe-head
   dataframe-modify
   dataframe-names
   dataframe-rename-all
   dataframe-partition
   dataframe-read
   dataframe-rename
   dataframe-select
   dataframe-sort
   dataframe-split
   dataframe-ref
   dataframe-tail
   dataframe-unique
   dataframe-update
   dataframe-values
   dataframe-values-unique
   dataframe-view
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
  ;; vals: one of the lists in the alist, but with the name excluded
  ;; ls-vals: vals from alist packaged into a list, e.g., '((1 2 3) ("this" "that" "other"))
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
    (let ([ls-vals (alist-values-map alist names)])
      (add-names-ls-vals
       names
       (map (lambda (vals)
              (map (lambda (n) (list-ref vals n)) indices))
            ls-vals))))

  ;; view -----------------------------------------------------------------------------------

  (define (dataframe-view df)
    (check-dataframe df "(dataframe-view df)")
    (let* ([rows (car (dataframe-dim df))]
           [n (if (< rows 10) rows 10)])
      (alist-head-tail (dataframe-alist df) n list-head)))
  
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
      (make-dataframe (alist-head-tail (dataframe-alist df) n proc))))

  (define (alist-head-tail alist n proc)
    (map (lambda (col) (cons (car col) (proc (cdr col) n))) alist))
  
  ;; rename columns ---------------------------------------------------------------------------------

  ;; name-pairs is of form '((old-name1 new-name1) (old-name2 new-name2))
  (define (dataframe-rename df name-pairs)
    (let ([proc-string "(dataframe df name-pairs)"])
      (check-dataframe df proc-string)
      (check-name-pairs (dataframe-names df) name-pairs proc-string))
    (make-dataframe (map (lambda (column)
                           (let* ([name (car column)]
                                  [ls-vals (cdr column)]
                                  [name-match (assoc name name-pairs)])
                             (if name-match
                                 (cons (cadr name-match) ls-vals)
                                 column)))
                         (dataframe-alist df))))

  (define (dataframe-rename-all df names)
    (let ([proc-string "(dataframe-rename-all df names)"])
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
             [ls-vals (map cdr alist)])
        (make-dataframe (add-names-ls-vals names ls-vals)))))

  ;; add names to list of vals, ls-vals, to create association list
  (define (add-names-ls-vals names ls-vals)
    (if (null? ls-vals)
        (map (lambda (name) (cons name '())) names)
        (map (lambda (name vals) (cons name vals)) names ls-vals)))

  ;; append -----------------------------------------------------------------------------------
  
  (define (dataframe-append . dfs)
    (let ([proc-string "(dataframe-append dfs)"]
          [rows (car (dataframe-dim (car dfs)))]
          [all-names (apply get-all-names dfs)])
      (check-all-dataframes dfs proc-string)
      (check-names-unique all-names proc-string)
      (unless (for-all (lambda (df) (= rows (car (dataframe-dim df)))) dfs)
        (assertion-violation proc-string "all dfs must have same number of rows")))
    (make-dataframe (apply append (map (lambda (df) (dataframe-alist df)) dfs))))
  
  ;; bind -----------------------------------------------------------------------------------

  (define (dataframe-bind . dfs)
    (let ([proc-string "(dataframe-bind dfs)"])
      (check-all-dataframes dfs proc-string)
      (let ([names (apply shared-names dfs)])
        (when (null? names) (assertion-violation proc-string "no names in common across dfs"))
        (let ([alist (map (lambda (name)
                            ;; missing-value will not be used so chose arbitrary value (-999)
                            (cons name (apply bind-rows name -999 dfs)))
                          names)])
          (make-dataframe alist)))))
  
  (define (shared-names . dfs)
    (let ([first-names (dataframe-names (car dfs))]
          [rest-names (apply get-all-unique-names (cdr dfs))])
      (filter (lambda (name) (member name rest-names)) first-names)))

  (define (dataframe-bind-all missing-value . dfs)
    (check-all-dataframes dfs "(dataframe-bind-all missing-value dfs)")
    (let* ([names (apply combine-names-ordered dfs)]
           [alist (map (lambda (name)
                         (cons name (apply bind-rows name missing-value dfs)))
                       names)])
      (make-dataframe alist)))

  (define (bind-rows name missing-value . dfs)
    (apply append
           (map (lambda (df)
                  (if (dataframe-contains? df name)
                      (dataframe-values df name)
                      (make-list (car (dataframe-dim df)) missing-value))) 
                dfs)))

  (define (get-all-names . dfs)
    (apply append (map (lambda (df) (dataframe-names df)) dfs)))

  (define (get-all-unique-names . dfs)
    (remove-duplicates (apply get-all-names dfs)))

  ;; combine names such that they stay in the order that they appear in each dataframe
  (define (combine-names-ordered . dfs)
    (define (loop all-names results)
      (cond [(null? all-names)
             (reverse results)]
            [(member (car all-names) results)
             (loop (cdr all-names) results)]
            [else
             (loop (cdr all-names) (cons (car all-names) results))]))
    (loop (apply get-all-names dfs) '()))
  
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

  (define (dataframe-values-unique df name)
    (let ([proc-string "(dataframe-values-unique df name)"])
      (check-dataframe df proc-string)
      (check-names-exist df proc-string name))
    (cdar (alist-unique (alist-select (dataframe-alist df) (list name)))))

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

  ;; filter-ls-vals involves zipping, filtering, and unzipping every column
  ;; alternative is to transpose to row-based,
  ;; cons bools to rows,
  ;; filter by car of rows,
  ;; cdr to remove bools,
  ;; and transpose back to column-based
  
  ;; I opted for zip/filter/unzip because the code is so simple
  ;; current approach might be more efficient when lots of rows and few columns
  ;; other approach might be more efficient when lots of columns and few rows
  ;; but I don't have a good sense of relative cost of the two approaches
  
  (define (dataframe-filter df filter-expr)
    (let* ([bools (filter-map df filter-expr)]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)]
           [new-ls-vals (filter-ls-vals bools (map cdr alist))])
      (make-dataframe (add-names-ls-vals names new-ls-vals))))

  (define-syntax filter-expr
    (syntax-rules ()
      [(_ names expr)
       (list (quote names) (lambda names expr))]))

  (define (filter-map df filter-expr)
    (let ([names (car filter-expr)]
          [proc (cadr filter-expr)])
      (apply check-df-names df "(dataframe-filter df filter-expr)" names)
      (apply map proc (dataframe-values-map df names))))

  ;; filter vals by list of booleans of same length as vals
  (define (filter-vals bools vals)
    (let ([bools-vals (map cons bools vals)])
      (map cdr (filter (lambda (x) (car x)) bools-vals))))
  
  (define (filter-ls-vals bools ls-vals)
    (map (lambda (vals) (filter-vals bools vals)) ls-vals))

  (define (dataframe-partition df filter-expr)
    (let* ([bools (filter-map df filter-expr)]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)])
      (let-values ([(keep drop) (partition-ls-vals bools (map cdr alist))])
        (values (make-dataframe (add-names-ls-vals names keep))
                (make-dataframe (add-names-ls-vals names drop))))))

  ;; two passes through ls-vals
  ;; recursive solution might be more efficient
  ;; currently just a shorthand way of writing two filters
  (define (partition-ls-vals bools ls-vals)
    (let ([keep (filter-ls-vals bools ls-vals)]
          [drop (filter-ls-vals (map not bools) ls-vals)])
      (values keep drop)))
    
  ;; sort ------------------------------------------------------------------------

  ;; as a reminder for myself...
  ;; < as a sort predicate is interpreted as increasing
  ;; i.e., thing on left is smaller than thing on right

  ;; like filter/partition, sort works on columns
  ;; might be less efficient on dataframes with many columns
  ;; than a row-based approach

  (define (dataframe-sort df sort-expr)
    (let* ([predicates (car sort-expr)]
           [names (cadr sort-expr)]
           [alist (dataframe-alist df)]
           [all-names (map car alist)]
           [ranks (sum-row-ranks alist predicates names)]
           [ls-vals-sorted (sort-ls-vals ranks (map cdr alist))])
      (make-dataframe (add-names-ls-vals all-names ls-vals-sorted))))

  (define-syntax sort-expr
    (syntax-rules ()
      [(_ (predicate name) ...)
       (list
        (list predicate ...)
        (list (quote name) ...))]))

  ;; sort list of values in increasing order by ranks
  (define (sort-vals ranks vals)
    (let ([ranks-vals (map cons ranks vals)])
      (map cdr (sort (lambda (x y) (< (car x) (car y))) ranks-vals))))
    
  (define (sort-ls-vals ranks ls-vals)
    (map (lambda (vals) (sort-vals ranks vals)) ls-vals))
  
  ;; returns the weighted sum of the ranks for each row
  (define (sum-row-ranks alist predicates names)
    (let* ([ls-vals (alist-values-map alist names)]
           [weights (map (lambda (x) (expt 10 x)) (enumerate names))]
           [ls-ranks (map (lambda (predicate ls weight)
                            (rank-list predicate ls weight))
                          predicates ls-vals weights)])
      (apply map + ls-ranks)))

  ;; returns list of weighted rank values for every value in ls 
  (define (rank-list predicate ls weight)
    (let* ([unique-sorted (sort predicate (remove-duplicates ls))]
           [ranks (map (lambda (x) (/ x weight)) (enumerate unique-sorted))]
           [lookup (map cons unique-sorted ranks)]) 
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
          [ls-vals (map cdr alist)])
      (add-names-ls-vals names (ls-vals-unique ls-vals #f))))

  (define (ls-vals-unique ls-vals row-based)
    (let ([ls-rows (remove-duplicates (transpose ls-vals))])
      (if row-based ls-rows (transpose ls-rows))))

  (define (transpose ls)
    (apply map list ls))

  ;; split ------------------------------------------------------------------------
  ;; might be worth the effort to make code for split more clear

  (define (dataframe-split df . group-names)
    (dataframe-split-helper df group-names #f))

  (define (dataframe-split-helper df group-names return-groups?)
    (apply check-df-names df "(dataframe-split df group-names)" group-names)
    (let-values ([(alists groups) (alist-split (dataframe-alist df) group-names #t)])
      (let ([dfs (map make-dataframe alists)])
        (if return-groups?
            (values dfs groups)
            dfs))))

  ;; returns two values
  ;; first value is list of alists representing all columns in the dataframe
  ;; second value is a list of alists representing the grouping columns in the dataframe
  (define (alist-split alist group-names return-groups?)
    (let* ([ls-vals-select (map cdr (alist-select alist group-names))]
           [ls-rows-unique (ls-vals-unique ls-vals-select #t)])
      (let-values ([(alists groups) (alist-split-partition-loop ls-rows-unique alist group-names '() '())])
        (if return-groups?
            (values alists groups)
            alists))))

  (define (alist-split-partition-loop ls-rows-unique alist group-names alists groups)
    (cond [(null? ls-rows-unique)
           (values (reverse alists)
                   (reverse groups))]
          [else
           ;; group-vals is a single row of vals representing one unique grouping combination
           (let ([group-vals (car ls-rows-unique)]) 
             (let-values ([(keep drop) (alist-split-partition group-vals group-names alist)])
               (alist-split-partition-loop (cdr ls-rows-unique)
                                           drop
                                           group-names
                                           (cons keep alists)
                                           (cons (add-names-ls-vals
                                                  group-names
                                                  (transpose (list group-vals)))
                                                 groups))))]))
    
  (define (alist-split-partition ls group-names alist)
    (let ([names (map car alist)]
          [ls-vals (map cdr alist)]
          [bools (map-equal-ls-vals
                  ls
                  (map cdr (alist-select alist group-names)))])
      (let-values ([(keep drop) (partition-ls-vals bools ls-vals)])
        (values (add-names-ls-vals names keep)
                (add-names-ls-vals names drop)))))

  ;; objective is to identify rows from ls-vals where
  ;; value in every column matches comparison value
  ;; bools in the let are 0s and 1s (not #f and #t)
  (define (map-equal-ls-vals comp-vals ls-vals)
    (let ([num-cols (length ls-vals)]
          [bools (map (lambda (comp-val vals) (map-equal comp-val vals))
                       comp-vals
                       ls-vals)])
      (map (lambda (sum) (= num-cols sum)) (apply map + bools))))
  
  ;; returns list of 0s and 1s (#f and #t) of same length as vals
  ;; boolean list used to identify rows that are equal to comparison value, comp-val
  (define (map-equal comp-val vals)
    (let ([pred (cond
                 [(number? comp-val) =]
                 [(string? comp-val) string=?]
                 [(symbol? comp-val) symbol=?]
                 [else equal?])])
      (map (lambda (val) (if (pred comp-val val) 1 0)) vals)))

  ;; modify/add columns ------------------------------------------------------------------------

  (define (dataframe-update df procedure . names)
    (apply check-df-names df "(dataframe-update df procedure names)" names)
    (make-dataframe (map (lambda (column)
                           (if (member (car column) names)
                               (cons (car column) (map procedure (cdr column)))
                               column))
                         (dataframe-alist df))))

  (define-syntax modify-expr
    (syntax-rules ()
      [(_ (new-name names expr) ...)
       (list
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))

  (define (dataframe-modify df modify-expr)
    (let ([alist (dataframe-alist df)]
          [new-names (car modify-expr)]
          [names-list (cadr modify-expr)]
          [proc-list (caddr modify-expr)]
          [proc-string "(dataframe-modify df modify-expr)"])
      (check-dataframe df proc-string)
      (check-names new-names proc-string)
      (make-dataframe
       (alist-modify-loop alist new-names names-list proc-list proc-string))))

  ;; can't just map over columns because won't hold alist structure
  ;; also don't want to map over procedures
  ;; because want to update alist after each procedure
  (define (alist-modify-loop alist new-names names-list proc-list who)
    (if (null? new-names)
        alist
        (alist-modify-loop (alist-modify alist
                                         (car new-names)
                                         (modify-map alist
                                                     (car names-list)
                                                     (car proc-list)
                                                     who))
                           (cdr new-names)
                           (cdr names-list)
                           (cdr proc-list)
                           who)))
  
  ;; update alist and or add column to end of alist
  ;; based on whether name is already present in alist
  (define (alist-modify alist name vals)
    (let ([col (cons name vals)]
          [all-names (map car alist)])
      (if (member name all-names)
          (map (lambda (x)
                 (if (symbol=? x name) col (assoc x alist)))
               all-names)
          ;; to get correct structure with append,
          ;; need to make col an alist (by wrapping in a list) 
          (append alist (list col))))) 

  (define (modify-map alist names proc who)
    (if (null? names)
        (modify-map-helper alist (proc) who)
        (apply map proc (alist-values-map alist names))))

  ;; this helper procedure returns vals for a column from a scalar or list of same length as number of df rows
  (define (modify-map-helper alist vals who)
    (let ([alist-rows (length (cdar alist))])
      (cond [(scalar? vals)
             (make-list alist-rows vals)]
            [(and (list? vals) (= (length vals) alist-rows))
             vals]
            [else
             (assertion-violation
              who
              (string-append
               "value(s) must be scalar or list of length "
               (number->string alist-rows)))])))

  (define (scalar? obj)
    (or (symbol? obj) (char? obj) (string? obj) (number? obj)))

  ;; (dataframe-list-modify) doesn't work when making a "non-vectorized" calculation, e.g., (mean ($ df 'count), and, thus, doesn't seem that useful 
  ;; (define (dataframe-list-modify df-list modify-expr)
  ;;   (apply dataframe-bind (map (lambda (df) (dataframe-modify df modify-expr)) df-list)))

  ;; aggregate  ------------------------------------------------------------------------

  ;; aggregate procedures are subtly different from modify procedures
  ;; decided have procedures with redundant functionality rather than working out how to generalize them
  
  ;; same macro as for modify-expr, but named to match the aggregate procedure
  (define-syntax aggregate-expr
    (syntax-rules ()
      [(_ (new-name names expr) ...)
       (list
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))
  
  (define (dataframe-aggregate df group-names aggregate-expr)
    (let ([proc-string "(dataframe-aggregate df group-names aggregate-expr)"])
      (check-dataframe df proc-string)
      (check-list group-names "group-names" proc-string)
      (let-values ([(df-list groups-list) (dataframe-split-helper df group-names #t)])
        (apply dataframe-bind
               (map (lambda (df groups)
                      (df-aggregate-helper df groups aggregate-expr proc-string))
                    df-list groups-list)))))

  (define (df-aggregate-helper df groups aggregate-expr who)
    ;; groups is an alist with one row
    (let* ([new-names (car aggregate-expr)]
           [names-list (cadr aggregate-expr)]
           [proc-list (caddr aggregate-expr)]
           [ls-vals (aggregate-map df names-list proc-list)])
      (check-names new-names who)
      (make-dataframe
       (alist-aggregate-loop groups new-names ls-vals))))

  (define (aggregate-map df names-list proc-list)
    (map (lambda (names proc)
           (list (apply proc (dataframe-values-map df names))))
         names-list proc-list))

  ;; can't just map over columns because won't hold alist structure
  (define (alist-aggregate-loop groups new-names ls-vals)
    (if (null? new-names)
        groups
        (alist-aggregate-loop (alist-modify groups
                                            (car new-names)
                                            (car ls-vals))
                              (cdr new-names)
                              (cdr ls-vals))))
  
  ;; rowtable ------------------------------------------------------------------------

  ;; rowtable is a bad name to describe list of rows; as used in read-csv and write-csv in (chez-stats csv)

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)])
      (cons names (transpose ls-vals))))

  (define (rowtable->dataframe rt header?)
    (check-rowtable rt "(rowtable->dataframe rt header?)")
    (let ([names (if header?
                     (car rt)
                     (map string->symbol
                          (map string-append
                               (make-list (length (car rt)) "V")
                               (map number->string
                                    (enumerate (car rt))))))]
          [ls-vals (if header?
                       (transpose (cdr rt))
                       (transpose rt))])
      (make-dataframe (map cons names ls-vals))))

  )

