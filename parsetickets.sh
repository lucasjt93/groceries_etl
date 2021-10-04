#!/bin/sh
# go to tickets folders, convert *.pdf to .txt and delete *.pdf
cd ~/consum/consum_project/data/tickets_pdf
if [ -e *.pdf ]; then
  for file in *.pdf; do
    base=${file%.*}
    pdftotext -layout "$file" "$base.txt"
    rm $file
  done;
fi
