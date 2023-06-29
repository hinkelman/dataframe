(library (dataframe aggregate)
  (export dataframe-aggregate
          dataframe-aggregate*)

  (import (rnrs)
          (dataframe bind)
          (dataframe split)
          (only (dataframe df)
                check-dataframe
                make-dataframe)   
          (only (dataframe helpers)
                add-names-ls-vals
                alist-ref
                alist-select
                alist-values-map
                check-list
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
        (list (lambda names expr) ...)
        "(dataframe-aggregate* df group-names (new-name names expr) ...)")]))
  
  (define (dataframe-aggregate df group-names new-names names . procedure)
    (df-aggregate df group-names new-names names procedure
                  "(dataframe-aggregate df group-names new-names procedure ...)"))

  (define (df-aggregate df group-names new-names names procs who)
    (check-dataframe df who)
    (check-names new-names who)
    (check-list group-names "group-names" who)
    ;; alists is a list of alists
    (let ([alists (dataframe-split-helper2 df group-names who)])
      (apply dataframe-bind
             (map (lambda (alist)
                    (df-aggregate-helper alist group-names new-names names procs))
                  alists))))
    
  (define (df-aggregate-helper alist group-names new-names names procs)
    ;; all values in the group columns of a split alist will be the same
    ;; need just the first row (index 0)
    (let ([alist-grp (alist-ref alist '(0) group-names)]
          [alist-agg (aggregate-map alist new-names names procs)])
      (make-dataframe (append alist-grp alist-agg))))

  ;; names-list would be, e.g., '((col1 col2) (col2 col3))
  ;; proc-list would be, e.g.,  (list (lambda (col1 col2) (do something))
  ;;                                  (lamdda (col2 col3) (do something else)))
  ;; returns a single row alist with the aggregated values
  (define (aggregate-map alist new-names names-list proc-list)
    (add-names-ls-vals
     new-names
     (map (lambda (names proc)
            (list (apply proc (alist-values-map alist names))))
          names-list proc-list)))
  )
