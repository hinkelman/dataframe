(library (dataframe join)
  (export dataframe-left-join
          dataframe-left-join-all)

  (import (rnrs)
          (dataframe bind)
          (dataframe split)
          (only (dataframe df)
                check-all-dataframes
                make-dataframe
                dataframe-names
                dataframe-contains?)   
          (only (dataframe helpers)
                make-list
                not-in
                check-names-unique
                alist-drop
                alist-ref
                alist-repeat-rows))

  (define (dataframe-left-join-all dfs join-names missing-value)
    (let loop ([dfs (cdr dfs)]
               [out (car dfs)])
      (if (null? dfs)
          out
          (loop (cdr dfs)
                (dataframe-left-join
                 out (car dfs) join-names missing-value)))))

  (define (dataframe-left-join df1 df2 join-names missing-value)
    (let ([who "(dataframe-left-join df1 df2 join-names missing-value)"])
      (check-all-dataframes (list df1 df2) who)
      (check-join-names-exist df1 "df1" who join-names)
      (check-join-names-exist df2 "df2" who join-names)
      (let ([df1-not-join-names
             (not-in (dataframe-names df1) join-names)]
            [df2-not-join-names
             (not-in (dataframe-names df2) join-names)])
        (check-names-unique (append df1-not-join-names df2-not-join-names) who))
      (let ([alists1 (dataframe-split-helper2 df1 join-names who)]
            [alists2 (dataframe-split-helper2 df2 join-names who)])
        (apply dataframe-bind
               (df-left-join-helper alists1 alists2 join-names missing-value)))))

  (define (check-join-names-exist df df-name who names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who (string-append "not all join-names in " df-name))))

  (define (df-left-join-helper alists1 alists2 join-names missing-value)
    (map (lambda (alist)
           ;; grp is a simple list
           ;; grps2 is a list of lists equal to the length of alists2
           (let* ([grp (get-join-group alist join-names)]
                  [grps2 (map (lambda (x) (get-join-group x join-names)) alists2)]
                  [grps2-alists2 (map (lambda (grp alist) (cons grp alist)) grps2 alists2)]
                  [grp-match (assoc grp grps2-alists2)])
             (if grp-match
                 ;; the cdr of grp-match is the alist2 that has the same grps
                 (join-match alist (cdr grp-match) join-names missing-value)
                 ;; all alists in alists2 have the same columns so just need one
                 (join-no-match alist (car alists2) join-names missing-value))))
         alists1))

  ;; takes an alist and returns the values from the first row of the join columns
  ;; assumes that all values in join-names columns are the same because alist was split
  (define (get-join-group alist join-names)
    (map cadr (alist-ref alist '(0) join-names)))

  ;; drop join-names columns from alist2 so that they aren't duplicated in the append
  ;; repeat rows for whichever alist has fewer
  (define (join-match alist1 alist2 join-names missing-value)
    (let* ([n1 (length (cdar alist1))]
           [n2 (length (cdar alist2))]
           [alist2-drop (alist-drop alist2 join-names)]
           [alist1-new (if (>= n1 n2)
                           alist1
                           (alist-repeat-rows alist1 n2 'times))]
           [alist2-new (if (>= n2 n1)
                           alist2-drop
                           (alist-repeat-rows alist2-drop n1 'times))])
      (make-dataframe (append alist1-new alist2-new))))
  
  ;; row(s) in alist1 have no match in alist2
  ;; retain non-join columns in alist2 and fill each column with missing value
  ;; then append the two alists and make a dataframe
  (define (join-no-match alist1 alist2 join-names missing-value)
    (let* ([n (length (cdar alist1))]
           [alist2-names (map car alist2)]
           [alist2-names-sel (not-in alist2-names join-names)])
      (make-dataframe
       (append alist1 (alist-fill-missing alist2-names-sel n missing-value)))))

  (define (alist-fill-missing names n missing-value)
    (map (lambda (name) (cons name (make-list n missing-value))) names))

  )

