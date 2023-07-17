(library (dataframe io)
  (export )

  (import (rnrs)
          (dataframe record-types))

  ;; need to be updated for slist structure
  ;; (define (dataframe-write df path overwrite)
  ;;   (when (and (file-exists? path) (not overwrite))
  ;;     (assertion-violation path "file already exists"))
  ;;   (when (file-exists? path)
  ;;     (delete-file path))
  ;;   (with-output-to-file path
  ;;     (lambda () (write (dataframe-alist df)))))

  ;; (define (dataframe-read path)
  ;;   (make-dataframe (with-input-from-file path read)))
  )

