package de.tuberlin.aura.demo.deployment;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;

public final class LocalDeployment {

	// ----------- MACHINE SETUP -----------

	// MACHINE_1
	public static final int MACHINE_1_DATA_PORT = 25335;

	public static final int MACHINE_1_MSG_PORT = 26335;

	public static final InetAddress MACHINE_1_ADDRESS = getLocalHost();

	public static final UUID MACHINE_1_UID = UUID.fromString("56765799-fbc5-4dbb-83a6-162d8a24a7ce");

	public static final MachineDescriptor MACHINE_1_DESCRIPTOR = new MachineDescriptor(MACHINE_1_UID,
		MACHINE_1_ADDRESS, MACHINE_1_DATA_PORT, MACHINE_1_MSG_PORT);

	// MACHINE_2
	public static final int MACHINE_2_DATA_PORT = 25336;

	public static final int MACHINE_2_MSG_PORT = 26336;

	public static final InetAddress MACHINE_2_ADDRESS = getLocalHost();

	public static final UUID MACHINE_2_UID = UUID.fromString("47022799-fbc5-4dbb-83a6-162d8a24a7ce");

	public static final MachineDescriptor MACHINE_2_DESCRIPTOR = new MachineDescriptor(MACHINE_2_UID,
		MACHINE_2_ADDRESS, MACHINE_2_DATA_PORT, MACHINE_2_MSG_PORT);

	// MACHINE_3
	public static final int MACHINE_3_DATA_PORT = 25337;

	public static final int MACHINE_3_MSG_PORT = 26337;

	public static final InetAddress MACHINE_3_ADDRESS = getLocalHost();

	public static final UUID MACHINE_3_UID = UUID.fromString("78922799-fbc5-4dbb-83a6-162d8a24a7ce");

	public static final MachineDescriptor MACHINE_3_DESCRIPTOR = new MachineDescriptor(MACHINE_3_UID,
		MACHINE_3_ADDRESS, MACHINE_3_DATA_PORT, MACHINE_3_MSG_PORT);

	// MACHINE_4
	public static final int MACHINE_4_DATA_PORT = 25338;

	public static final int MACHINE_4_MSG_PORT = 26338;

	public static final InetAddress MACHINE_4_ADDRESS = getLocalHost();

	public static final UUID MACHINE_4_UID = UUID.fromString("36822799-fbc5-4dbb-83a6-162d8a24a7ce");

	public static final MachineDescriptor MACHINE_4_DESCRIPTOR = new MachineDescriptor(MACHINE_4_UID,
		MACHINE_4_ADDRESS, MACHINE_4_DATA_PORT, MACHINE_4_MSG_PORT);

	// MACHINE_5
	public static final int MACHINE_5_DATA_PORT = 25339;

	public static final int MACHINE_5_MSG_PORT = 26339;

	public static final InetAddress MACHINE_5_ADDRESS = getLocalHost();

	public static final UUID MACHINE_5_UID = UUID.fromString("96852799-fbc5-4dbb-83a6-162d8a24a7ce");

	public static final MachineDescriptor MACHINE_5_DESCRIPTOR = new MachineDescriptor(MACHINE_5_UID,
		MACHINE_5_ADDRESS, MACHINE_5_DATA_PORT, MACHINE_5_MSG_PORT);

	// MACHINE_6
	public static final int MACHINE_6_DATA_PORT = 25340;

	public static final int MACHINE_6_MSG_PORT = 26340;

	public static final InetAddress MACHINE_6_ADDRESS = getLocalHost();

	public static final UUID MACHINE_6_UID = UUID.fromString("96852799-ccc5-4dbb-83a6-982d8a24a7ce");

	public static final MachineDescriptor MACHINE_6_DESCRIPTOR = new MachineDescriptor(MACHINE_6_UID,
		MACHINE_6_ADDRESS, MACHINE_6_DATA_PORT, MACHINE_6_MSG_PORT);

	/*
	 * //----------- TASK SETUP -----------
	 * // TASK_1
	 * public static final UUID TASK_1_UID = UUID.fromString( "50810180-6ca4-433e-ae2b-422e78a69fd5" );
	 * public static final String TASK_1_NAME = "TASK_1";
	 * public static final TaskDescriptor TASK_1_DESCRIPTOR = new TaskDescriptor( TASK_1_UID, TASK_1_NAME, USER_CODE );
	 * // TASK_2
	 * public static final UUID TASK_2_UID = UUID.fromString( "445a4ba2-e5c0-4afc-a503-7cbbda11fb07" );
	 * public static final String TASK_2_NAME = "TASK_2";
	 * public static final TaskDescriptor TASK_2_DESCRIPTOR = new TaskDescriptor( TASK_2_UID, TASK_2_NAME, USER_CODE );
	 * // TASK_3
	 * public static final UUID TASK_3_UID = UUID.fromString( "456434ba-e5c0-4afc-a503-7cbbda11fb07" );
	 * public static final String TASK_3_NAME = "TASK_3";
	 * public static final TaskDescriptor TASK_3_DESCRIPTOR = new TaskDescriptor( TASK_3_UID, TASK_3_NAME, USER_CODE );
	 * // TASK_4
	 * public static final UUID TASK_4_UID = UUID.fromString( "956434ba-e5c0-4afc-a503-7cbbda11fb07" );
	 * public static final String TASK_4_NAME = "TASK_4";
	 * public static final TaskDescriptor TASK_4_DESCRIPTOR = new TaskDescriptor( TASK_4_UID, TASK_4_NAME, USER_CODE );
	 * // TASK_5
	 * public static final UUID TASK_5_UID = UUID.fromString( "956434ba-e5c0-4afc-a703-7cbbda11fb07" );
	 * public static final String TASK_5_NAME = "TASK_5";
	 * public static final TaskDescriptor TASK_5_DESCRIPTOR = new TaskDescriptor( TASK_5_UID, TASK_5_NAME, USER_CODE );
	 * // TASK_6
	 * public static final UUID TASK_6_UID = UUID.fromString( "956434ba-5290-4afc-a703-7cbbda11fb07" );
	 * public static final String TASK_6_NAME = "TASK_6";
	 * public static final TaskDescriptor TASK_6_DESCRIPTOR = new TaskDescriptor( TASK_6_UID, TASK_6_NAME, USER_CODE );
	 * //----------- TASK BINDING SETUP -----------
	 * // TASK_1_BINDING
	 * public static final TaskDescriptor[] TASK_1_INPUT = { };
	 * public static final TaskDescriptor[] TASK_1_OUTPUT = { TASK_3_DESCRIPTOR };
	 * public static final TaskBindingDescriptor TASK_1_BINDING = new TaskBindingDescriptor( TASK_1_DESCRIPTOR,
	 * Arrays.asList( TASK_1_INPUT ), Arrays.asList( TASK_1_OUTPUT ) );
	 * // TASK_2_BINDING
	 * public static final TaskDescriptor[] TASK_2_INPUT = { };
	 * public static final TaskDescriptor[] TASK_2_OUTPUT = { TASK_3_DESCRIPTOR };
	 * public static final TaskBindingDescriptor TASK_2_BINDING = new TaskBindingDescriptor( TASK_2_DESCRIPTOR,
	 * Arrays.asList( TASK_2_INPUT ), Arrays.asList( TASK_2_OUTPUT ) );
	 * // TASK_3_BINDING
	 * public static final TaskDescriptor[] TASK_3_INPUT = { TASK_1_DESCRIPTOR, TASK_2_DESCRIPTOR };
	 * public static final TaskDescriptor[] TASK_3_OUTPUT = { TASK_4_DESCRIPTOR };
	 * public static final TaskBindingDescriptor TASK_3_BINDING = new TaskBindingDescriptor( TASK_3_DESCRIPTOR,
	 * Arrays.asList( TASK_3_INPUT ), Arrays.asList( TASK_3_OUTPUT ) );
	 * // TASK_4_BINDING
	 * public static final TaskDescriptor[] TASK_4_INPUT = { TASK_3_DESCRIPTOR };
	 * public static final TaskDescriptor[] TASK_4_OUTPUT = { };
	 * public static final TaskBindingDescriptor TASK_4_BINDING = new TaskBindingDescriptor( TASK_4_DESCRIPTOR,
	 * Arrays.asList( TASK_4_INPUT ), Arrays.asList( TASK_4_OUTPUT ) );
	 * // TASK_5_BINDING
	 * public static final TaskDescriptor[] TASK_5_INPUT = { };
	 * public static final TaskDescriptor[] TASK_5_OUTPUT = { TASK_6_DESCRIPTOR };
	 * public static final TaskBindingDescriptor TASK_5_BINDING = new TaskBindingDescriptor( TASK_5_DESCRIPTOR,
	 * Arrays.asList( TASK_5_INPUT ), Arrays.asList( TASK_5_OUTPUT ) );
	 * // TASK_6_BINDING
	 * public static final TaskDescriptor[] TASK_6_INPUT = { TASK_5_DESCRIPTOR };
	 * public static final TaskDescriptor[] TASK_6_OUTPUT = { };
	 * public static final TaskBindingDescriptor TASK_6_BINDING = new TaskBindingDescriptor( TASK_6_DESCRIPTOR,
	 * Arrays.asList( TASK_6_INPUT ), Arrays.asList( TASK_6_OUTPUT ) );
	 */

	private static InetAddress getLocalHost() {
		try {
			return InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			return null;
		}
	}
}
