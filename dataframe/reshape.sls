(library (dataframe reshape)
  (export dataframe-stack
          dataframe-spread)

  (import (rnrs)
          (dataframe join)
          (dataframe split)
          (dataframe record-types)
          (only (dataframe filter)
                dataframe-unique
                slist-ref)
          (only (dataframe select)
                dataframe-select
                slist-drop
                slist-select
                $)
          (only (dataframe helpers)
                not-in
                rep))

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
           [slist-rep (slist-repeat-rows
                       (slist-select (dataframe-slist df) other-names)
                       (length names)
                       'times)]
           [nrows (car (dataframe-dim df))]
           [names-to-vals (rep names nrows 'each)]
           [slist-sel (slist-select (dataframe-slist df) names)]
           ;; stack the values from each column
           [values-to-vals (apply append (map series-lst slist-sel))])
      (make-dataframe
       (append slist-rep
               (list (make-series names-to names-to-vals))
               (list (make-series values-to values-to-vals))))))

  (define dataframe-spread
    (case-lambda
      [(df names-from values-from)
       (dataframe-spread df names-from values-from 'na)]
      [(df names-from values-from fill-value)
       (let ([who "(dataframe-spread df names-from values-from fill-value)"])
         (check-df-names df who names-from values-from)
         (unless (for-all (lambda (val) (or (symbol? val) (string? val))) ($ df names-from))
           (assertion-violation who "values in names-from must be strings or symbols"))
         (let* ([not-spread-names
                 (not-in (dataframe-names df) (list names-from values-from))]
                [left-df
                 (dataframe-unique (dataframe-select df not-spread-names))]
                [slists (dataframe-split-helper df (list names-from) who)]
                ;; for each slist, we need to rename the values-from column
                ;; with the first value in the names-from (nf) column
                ;; b/c split slist all have same value
                ;; then we drop the names-from column and left-join all the new dfs
                [dfs (map (lambda (slist)
                            (let* ([nf1
                                    ;; the car's are annoying; need to do lots of unwrapping
                                    (car (series-lst
                                          (car (slist-ref slist '(0) (list names-from)))))]
                                   [nf2
                                    (if (symbol? nf1) nf1 (string->symbol nf1))]
                                   [ls-vals
                                    (map series-lst
                                         (slist-drop slist (list names-from)))])
                              (make-dataframe
                               (make-slist (append not-spread-names (list nf2)) ls-vals))))
                          slists)])
           (dataframe-left-join-all (cons left-df dfs) not-spread-names fill-value)))]))
  
  )

