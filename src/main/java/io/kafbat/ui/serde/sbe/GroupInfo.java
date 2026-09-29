package io.kafbat.ui.serde.sbe;


final class GroupInfo {

  private static final int NOT_SET = -1;

  private final String name;
  private final int minBlockLength;
  private final int actingVersion;

  private int previousEntryIndex = NOT_SET;
  private boolean currentEntryRecorded;

  GroupInfo(final String name, final int minBlockLength, final int actingVersion) {
    this.name = name;
    this.minBlockLength = minBlockLength;
    this.actingVersion = actingVersion;
  }

  void beginEntry() {
    currentEntryRecorded = false;
  }

  void onFieldIndex(final int index) {
    if (currentEntryRecorded) {
      return;
    }
    currentEntryRecorded = true;
    if (previousEntryIndex != NOT_SET && index - previousEntryIndex < minBlockLength) {
      throw new SbeRecordDecoder.DecodeException(
          "SBE group %s has entries %d bytes apart, but version %d needs at least %d"
              .formatted(name, index - previousEntryIndex, actingVersion, minBlockLength));
    }
    previousEntryIndex = index;
  }
}
