# Option 1
# aws glue get-workflow --name biz-insights-daily --include-graph \
# | jq -r '
#   .Workflow.Graph as $g
#   | ($g.Nodes | map({(.Id): (.Name + " [" + .Type + "]")}) | add) as $map
#   | $g.Edges[]
#   | "\($map[.SourceId]) -> \($map[.DestinationId])"
# '

# Option 2
# aws glue get-workflow --name biz-insights-daily --include-graph \
# | jq -r '.Workflow.Graph.Nodes[] | "\(.Id)\t\(.Name)\t[\(.Type)]"'

# Option 3
aws glue get-workflow --name biz-insights-daily --include-graph \
| jq -r '
  .Workflow.Graph as $g
  | ($g.Nodes | map({(.Id): {name: .Name, t: .Type}}) | add) as $N
  | "digraph G {",
    "  rankdir=LR;",
    "  node [shape=box, style=\"rounded,filled\", fillcolor=white];",
    # style by type
    ( $g.Nodes[] |
      "  \(.Id) [label=\"\(.Name)\n(\(.Type))\" " +
      (if .Type=="CRAWLER" then ",shape=ellipse"
       elif .Type=="TRIGGER" then ",shape=diamond"
       else "" end) + "];"
    ),
    ( $g.Edges[] | "  \(.SourceId) -> \(.DestinationId);" ),
    "}"
' > workflow.dot

# If you have graphviz installed:  brew install graphviz
dot -Tpng workflow.dot -o workflow.png

