(library (dataframe aggregate)
  (export dataframe-aggregate
          dataframe-aggregate*)

  (import (rnrs)
          (dataframe bind)
          (dataframe split)
          (only (dataframe filter)
                slist-ref)
          (only (dataframe select)
                slist-select)
          (dataframe record-types)  
          (only (dataframe assertions)
                check-names))

  ;; aggregate  ------------------------------------------------------------------------

  (define-syntax dataframe-aggregate*
    (syntax-rules ()
      [(_ df group-names (new-name names expr) ...)
       (df-aggregate
        df
        (quote group-names)
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))
  
  (define (dataframe-aggregate df group-names new-names names . procedure)
    (df-aggregate df group-names new-names names procedure))

  (define (df-aggregate df group-names new-names names procs)
    (let ([who "(dataframe-aggregate df group-names new-names procedure ...)"])
      (check-dataframe df who)
      (check-names new-names who)
      ;; slists is a list of slists
      (let ([slists (dataframe-split-helper df group-names who)])
        (dataframe-bind-all
         (map (lambda (slist)
                (df-aggregate-helper slist group-names new-names names procs))
              slists)))))
    
  (define (df-aggregate-helper slist group-names new-names names procs)
    ;; all values in the group columns of a split slist will be the same
    ;; need just the first row (index 0)
    (let ([slist-grp (slist-ref slist '(0) group-names)]
          [slist-agg (aggregate-map slist new-names names procs)])
      (make-dataframe (append slist-grp slist-agg))))

  ;; names-list would be, e.g., '((col1 col2) (col2 col3))
  ;; proc-list would be, e.g.,  (list (lambda (col1 col2) (do something))
  ;;                                  (lamdda (col2 col3) (do something else)))
  ;; returns a single row slist with the aggregated values
  (define (aggregate-map slist new-names names-list proc-list)
    (make-slist
     new-names
     (map (lambda (names proc)
            (list (apply proc (map series-lst (slist-select slist names)))))
          names-list proc-list)))
  )
