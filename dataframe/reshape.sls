(library (dataframe reshape)
  (export dataframe-stack
          dataframe-spread)

  (import (rnrs)
          (dataframe join)
          (dataframe split)
          (only (dataframe df)
                $
                check-df-names
                dataframe-alist
                dataframe-contains?
                dataframe-dim
                dataframe-names
                dataframe-select
                dataframe-unique
                dataframe-values-map
                make-dataframe)
          (only (dataframe helpers)
                add-names-ls-vals
                not-in
                rep
                alist-drop
                alist-ref
                alist-repeat-rows))

  (define (dataframe-stack df names names-to values-to)
    (let ([who "(dataframe-stack df names names-to values-to)"])
      (apply check-df-names df who names)
      (unless (symbol? names-to)
        (assertion-violation who "names-to must be symbol"))
      (unless (symbol? values-to)
        (assertion-violation who "values-to must be symbol"))
      (when (dataframe-contains? df names-to values-to)
        (assertion-violation who "names-to or values-to already exist in df")))
    (let* ([other-names (not-in (dataframe-names df) names)]
           [alist-rep (alist-repeat-rows
                       (dataframe-alist (apply dataframe-select df other-names))
                       (length names)
                       'times)]
           [nrows (car (dataframe-dim df))]
           [names-to-vals (rep names nrows 'each)]
           [values-to-vals (apply append (dataframe-values-map df names))])
      (make-dataframe
       (append alist-rep
               (list (cons names-to names-to-vals))
               (list (cons values-to values-to-vals))))))

  (define (dataframe-spread df names-from values-from missing-value)
    (let ([who "(dataframe-spread df names-from values-from missing-value)"])
      (check-df-names df who names-from values-from)
      (unless (for-all (lambda (val) (or (symbol? val) (string? val))) ($ df names-from))
        (assertion-violation who "values in names-from must be strings or symbols"))
      (let* ([not-spread-names
              (not-in (dataframe-names df) (list names-from values-from))]
             [left-df
              (dataframe-unique (apply dataframe-select df not-spread-names))]
             [alists (dataframe-split-helper df (list names-from) who)]
             ;; for each alist, we need to rename the values-from column
             ;; with the first value in the names-from (nf) column (b/c split alist all have same value)
             ;; then we drop the names-from column and left-join all the new dfs
             [dfs (map (lambda (alist)
                         (let* ([nf1 (cadar (alist-ref alist '(0) (list names-from)))]
                                [nf2 (if (symbol? nf1) nf1 (string->symbol nf1))]
                                [ls-vals (map cdr (alist-drop alist (list names-from)))])
                           (make-dataframe
                            (add-names-ls-vals (append not-spread-names (list nf2)) ls-vals))))
                       alists)])
        (dataframe-left-join-all (cons left-df dfs) not-spread-names missing-value))))
  
  )

