cd maelstrom-echo/maelstrom && chmod +x maelstrom && ./maelstrom test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10
cd maelstrom-unique-ids/maelstrom && chmod +x maelstrom && ./maelstrom test -w unique-ids --bin ~/go/bin/-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
