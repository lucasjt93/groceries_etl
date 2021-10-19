#!/bin/sh
# go to tickets folders, convert *.pdf to .txt and delete *.pdf
cd ~/consum/consum_project/data/tickets_pdf
for file in *.pdf; do
  if [ ! -e *.pdf ]; then
    echo "No pdf file"
    return 0
  fi
  base=${file%.*}
  pdftotext -layout "$file" "$base.txt"
  rm $file
done;
