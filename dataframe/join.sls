(library (dataframe join)
  (export dataframe-left-join
          dataframe-left-join-all)

  (import (rnrs)
          (dataframe bind)
          (dataframe split)
          (dataframe record-types)
          (only (dataframe filter)
                slist-ref)
          (only (dataframe select)
                slist-drop)
          (only (dataframe assertions)
                check-names-unique)
          (only (dataframe helpers)
                make-list
                not-in))

  (define dataframe-left-join-all
    (case-lambda
      [(dfs join-names)
       (dataframe-left-join-all dfs join-names 'na)]
      [(dfs join-names missing-value)
       (let loop ([dfs (cdr dfs)]
                  [out (car dfs)])
         (if (null? dfs)
             out
             (loop (cdr dfs)
                   (dataframe-left-join
                    out (car dfs) join-names missing-value))))]))

  (define dataframe-left-join
    (case-lambda
      [(df1 df2 join-names)
       (dataframe-left-join df1 df2 join-names 'na)]
      [(df1 df2 join-names missing-value)
       (let ([who "(dataframe-left-join df1 df2 join-names missing-value)"])
         (check-all-dataframes (list df1 df2) who)
         (check-join-names-exist df1 "df1" who join-names)
         (check-join-names-exist df2 "df2" who join-names)
         (let ([df1-not-join-names
                (not-in (dataframe-names df1) join-names)]
               [df2-not-join-names
                (not-in (dataframe-names df2) join-names)])
           (check-names-unique (append df1-not-join-names df2-not-join-names) who))
         (let ([slists1 (dataframe-split-helper df1 join-names who)]
               [slists2 (dataframe-split-helper df2 join-names who)])
           (dataframe-bind-all
            (df-left-join-helper slists1 slists2 join-names missing-value))))]))

  (define (check-join-names-exist df df-name who names)
    (unless (apply dataframe-contains? df names)
      (assertion-violation who (string-append "not all join-names in " df-name))))

  (define (df-left-join-helper slists1 slists2 join-names missing-value)
    (let* ([grps2 (map (lambda (x) (get-join-group x join-names)) slists2)]
           [grps2-slists2 (map (lambda (grp slist) (cons grp slist)) grps2 slists2)])
      (map (lambda (slist)
             ;; grp is a simple list
             ;; grps2 is a list of lists equal to the length of slists2
             (let* ([grp (get-join-group slist join-names)]
                    [grp-match (assoc grp grps2-slists2)])
               (if grp-match
                   ;; the cdr of grp-match is the slist2 that has the same grps
                   (join-match slist (cdr grp-match) join-names missing-value)
                   ;; all slists in slists2 have the same columns so just need one
                   (join-no-match slist (car slists2) join-names missing-value))))
           slists1)))

  ;; takes an slist and returns the values from the first row of the join columns
  ;; assumes that all values in join-names columns are the same because alist was split
  (define (get-join-group slist join-names)
    (apply append (map series-lst (slist-ref slist '(0) join-names))))

  ;; drop join-names columns from slist2 so that they aren't duplicated in the append
  ;; repeat rows for whichever slist has fewer
  (define (join-match slist1 slist2 join-names missing-value)
    (let* ([n1 (series-length (car slist1))]
           [n2 (series-length (car slist2))]
           [slist2-drop (slist-drop slist2 join-names)]
           [slist1-new (if (>= n1 n2)
                           slist1
                           (slist-repeat-rows slist1 n2 'times))]
           [slist2-new (if (>= n2 n1)
                           slist2-drop
                           (slist-repeat-rows slist2-drop n1 'times))])
      (make-dataframe (append slist1-new slist2-new))))
  
  ;; row(s) in slist1 have no match in slist2
  ;; retain non-join columns in slist2 and fill each column with missing value
  ;; then append the two slists and make a dataframe
  (define (join-no-match slist1 slist2 join-names missing-value)
    (let* ([n (series-length (car slist1))]
           [slist2-names (map series-name slist2)]
           [slist2-names-sel (not-in slist2-names join-names)])
      (make-dataframe
       (append slist1 (slist-fill-missing slist2-names-sel n missing-value)))))

  (define (slist-fill-missing names n missing-value)
    (map (lambda (name) (make-series name (make-list n missing-value))) names))

  )

