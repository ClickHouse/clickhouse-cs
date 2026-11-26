using System.Collections.Generic;
using System.Linq;

namespace ClickHouse.Driver.Types.Grammar;

public static class Parser
{
    public static SyntaxTreeNode Parse(string input)
    {
        var tokens = Tokenizer.GetTokens(input).ToList();
        var stack = new Stack<SyntaxTreeNode>();
        SyntaxTreeNode current = null;

        foreach (var token in tokens)
        {
            switch (token)
            {
                case "(":
                    // The parameter list is a new scope; keeping the parent node in current
                    // would make an empty list add the parent to its own child nodes
                    stack.Push(current);
                    current = null;
                    break;
                case ",":
                    AddParsedNode(stack, ref current);
                    break;
                case ")":
                    AddParsedNode(stack, ref current);
                    current = stack.Pop();
                    break;
                default:
                    current = new SyntaxTreeNode { Value = token };
                    break;
            }
        }
        return current;
    }

    private static void AddParsedNode(Stack<SyntaxTreeNode> stack, ref SyntaxTreeNode current)
    {
        if (current != null)
        {
            stack.Peek().ChildNodes.Add(current);
            current = null;
        }
    }
}
