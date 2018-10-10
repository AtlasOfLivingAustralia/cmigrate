package au.org.ala.cmigrate;

import org.apache.commons.cli.ParseException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link CMigrate}.
 */
public class CMigrateTest 
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
    /**
     * Test default parameters for everything
     */
	@Ignore("TODO: Start up a test cassandra to run this against")
	@Test
    public void testNoArgs() throws Exception
    {
		thrown.expect(ParseException.class);
		CMigrate.main();
    }

    /**
     * Test help
     */
	@Test
    public void testHelp() throws Exception
    {
		CMigrate.main("--help");
    }
}
