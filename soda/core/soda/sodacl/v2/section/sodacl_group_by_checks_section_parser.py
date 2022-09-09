from soda.sodacl.antlr.SodaCLAntlrParser import SodaCLAntlrParser
from soda.sodacl.v2.section.sodacl_group_by_checks_section import SodaClGroupByChecksSection


class SodaClGroupByChecksSectionParser:

    @staticmethod
    def parse_group_by_checks_section(
        antlr_group_by_checks_header: SodaCLAntlrParser.Group_by_checks_headerContext
    ) -> SodaClGroupByChecksSection:
        return SodaClGroupByChecksSection(
            section_header_line=antlr_group_by_checks_header.getText()
        )
