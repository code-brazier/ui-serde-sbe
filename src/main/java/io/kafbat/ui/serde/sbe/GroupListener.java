package io.kafbat.ui.serde.sbe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import uk.co.real_logic.sbe.ir.Token;

final class GroupListener {

  private final List<Token> tokens;
  private final int actingVersion;
  private final Deque<GroupInfo> groups = new ArrayDeque<>();

  GroupListener(final List<Token> tokens, final int actingVersion) {
    this.tokens = tokens;
    this.actingVersion = actingVersion;
  }

  void onGroupHeader(final Token groupToken, final int numInGroup) {
    if (numInGroup > 0) {
      final int minBlockLength = SbeSchemas.minBlockLength(tokens, getGroupTokenIndex(groupToken), actingVersion);
      groups.push(new GroupInfo(groupToken.name(), minBlockLength, actingVersion));
    }
  }

  void onBeginGroup() {
    groups.getFirst().beginEntry();
  }

  void onFieldIndex(final int index) {
    final GroupInfo group = groups.peek();
    if (group != null) {
      group.onFieldIndex(index);
    }
  }

  void onEndGroup(final int groupIndex, final int numInGroup) {
    if (groupIndex == numInGroup - 1) {
      groups.pop();
    }
  }

  private int getGroupTokenIndex(final Token groupToken) {
    for (int i = 0; i < tokens.size(); i++) {
      if (tokens.get(i) == groupToken) {
        return i + 1 + tokens.get(i + 1).componentTokenCount();
      }
    }
    throw new IllegalStateException("Group " + groupToken.name() + " isn't in the message's tokens");
  }
}
