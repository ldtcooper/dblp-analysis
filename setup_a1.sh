mkdir materials
cd materials
curl https://dblp.org/xml/release/dblp-2022-01-01.xml.gz > dblp-2022-01-01.xml.gz
curl https://dblp.uni-trier.de/xml/dblp.dtd > dblp.dtd
gzip -d dblp-2022-01-01.xml.gz
mv dblp-2022-01-01.xml dblp.xml
cd ..
cp config.json.template ./config.json
echo "Please open config.json and fill it in" 