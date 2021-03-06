import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.util.Scanner;


public class Main {
    private static boolean loggedIn = false;
    private static String userPhoneNumber;
    private static long pv_counter_id = 0;
    private static long group_counter_id = 0;
    private static long channel_counter_id = 0;

    public static void main(String[] args) throws Exception {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        SearchResponse response = client.prepareSearch("messages")
                .setTypes("pv")
                .setSize(0) // Don't return any documents, we don't need them.
                .get();

        SearchHits hits = response.getHits();
        long hitsCount = hits.getTotalHits();
        pv_counter_id = hitsCount;
//        System.out.println(pv_counter_id);
        response = client.prepareSearch("messages2")
                .setTypes("group")
                .setSize(0)
                .get();
        hits = response.getHits();
        hitsCount = hits.getTotalHits();
        group_counter_id = hitsCount;
//        System.out.println(group_counter_id);

        response = client.prepareSearch("messages3")
                .setTypes("channel")
                .setSize(0)
                .get();
        hits = response.getHits();
        hitsCount = hits.getTotalHits();
        channel_counter_id = hitsCount;

        while (true) {
            System.out.println("enter a command");
            Scanner input = new Scanner(System.in);
            String enteredCommand = input.nextLine();
            //System.out.println("next line"+enteredCommand);
            if (enteredCommand.startsWith("login")) {
                String requestPhoneNumber = enteredCommand.substring(6, enteredCommand.length());
                // System.out.println(loggedIn);
                User loginRequest = new User(requestPhoneNumber);
                boolean resultOfLoginRequest = loginRequest.existsUser();
                if (resultOfLoginRequest) {
                    String password = loginRequest.password();
                    if (password != null) {
                        String enterredPassword;
                        do {
                            System.out.println("Enter password");
                            Scanner getPassword = new Scanner(System.in);
                            enterredPassword = getPassword.nextLine();
                            if (enterredPassword.equals("quiet")) break;
                            if (enterredPassword.equals(password)) {
                                System.out.println("you are logged in");
                                loggedIn = resultOfLoginRequest;
                                userPhoneNumber = requestPhoneNumber;
                                loginRequest.updateLastLogin(userPhoneNumber);
                            }
                        } while (!enterredPassword.equals(password));

                    } else {
                        loggedIn = resultOfLoginRequest;
                        userPhoneNumber = requestPhoneNumber;
                        loginRequest.updateLastLogin(userPhoneNumber);
                    }


//                    loggedIn = resultOfLoginRequest;
//                    userPhoneNumber=requestPhoneNumber;
//                    //System.out.println(userPhoneNumber);
//                    //System.out.println(loggedIn);
                }
                //System.out.println(loggedIn);
            } else if (enteredCommand.startsWith("logout ") && loggedIn) {
                loggedIn = false;
                userPhoneNumber = "";

            } else if (enteredCommand.startsWith("set_bio ") && loggedIn) {
                user_settings bio = new user_settings(userPhoneNumber, enteredCommand.substring(8, enteredCommand.length()));
                //System.out.println(userPhoneNumber);
                //System.out.println(enteredCommand.substring(8,enteredCommand.length()));
                bio.setBio();


            } else if (enteredCommand.startsWith("set_self_destruct ") && loggedIn) {
                char valueOFSelfDestruct = enteredCommand.charAt(18);
                int s = Character.getNumericValue(valueOFSelfDestruct);
                //System.out.println(s);
                if (s <= 3 && s >= 0) {
                    user_settings selfDestruct = new user_settings(userPhoneNumber);
                    selfDestruct.setSelfDestruct(s);
                } else {
                    System.out.println("not valid input");
                }

            } else if (enteredCommand.startsWith("set_password") && loggedIn) {
                String pass = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                //System.out.println(pass);
                User setPassword = new User(userPhoneNumber);
                setPassword.setPassword(pass);

            } else if (enteredCommand.startsWith("block_user") && loggedIn) {
                String blockedNumber = enteredCommand.substring(11, enteredCommand.length());
                BlockedUser blockedUser = new BlockedUser();
                if (!userPhoneNumber.equals(blockedNumber)) {
                    blockedUser.addBlockedUser(blockedNumber, userPhoneNumber);
                } else {
                    System.out.println("you cant block yourself");
                }

            } else if (enteredCommand.startsWith("unblock_user ")) {
                String unblockedUser = enteredCommand.substring(13, enteredCommand.length());
                //System.out.println(unblockedUser);
                BlockedUser unblockUser = new BlockedUser();
                unblockUser.unblockUser(unblockedUser, userPhoneNumber);


            } else if (enteredCommand.startsWith("create_user ")) {
                userPhoneNumber = enteredCommand.substring(12, enteredCommand.length());
                if (userPhoneNumber.length() == 11) {
                    User user = new User(userPhoneNumber);
                    user.createUser();
                }

            } else if (enteredCommand.startsWith("set_name ") && loggedIn) {
                String name = enteredCommand.substring(9, enteredCommand.length());
                User setName = new User(userPhoneNumber);
                setName.setName(name);
            } else if (enteredCommand.startsWith("send_message ") && loggedIn) {
                String receiver = enteredCommand.substring(13, 24);
                String messageText = enteredCommand.substring(25, enteredCommand.length());
                //System.out.println(receiver);
                //System.out.println(messageText);
                PrivateChats message = new PrivateChats();
                message.sendMessage(userPhoneNumber, receiver, messageText, client, pv_counter_id);
                pv_counter_id++;

            } else if (enteredCommand.startsWith("create_channel ") && loggedIn) {
                String channelID = enteredCommand.substring(15, enteredCommand.lastIndexOf(" "));
                String channelName = enteredCommand.substring(enteredCommand.lastIndexOf(" ") + 1, enteredCommand.length());
                //System.out.println(channelID);
                //System.out.println(channelName);
                Channel newChannel = new Channel();
                newChannel.createChannel(channelID, channelName, userPhoneNumber);

            } else if (enteredCommand.startsWith("send_message_channel ") && loggedIn) {
                String arg = enteredCommand.substring(21, enteredCommand.length());
                String ID = arg.substring(0, arg.indexOf(" "));
                //System.out.println(ID);
                String messageText = arg.substring(arg.indexOf(" ") + 1, arg.length());
                //System.out.println(enteredCommand.indexOf(" "));
                Channel newChannel = new Channel();
                boolean isAdmin = newChannel.isAdmin(userPhoneNumber, ID);
                if (isAdmin) {
                    //System.out.println("if ");
                    ChannelMessage cm = new ChannelMessage();
                    cm.sendMessage(ID, messageText, client, channel_counter_id);
                    channel_counter_id++;


                } else {
                    System.out.println("Access denied");
                }

            } else if (enteredCommand.startsWith("create_group ") && loggedIn) {
                String arg = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                //System.out.println(arg);
                String groupID = arg.substring(0, arg.indexOf(" "));
                String groupName = arg.substring(arg.indexOf(" ") + 1, arg.length());
                // System.out.println(groupID);
                //System.out.println(groupName);
                Groupes newGroupes = new Groupes();
                newGroupes.createGroup(groupID, groupName, userPhoneNumber);


            } else if (enteredCommand.startsWith("send_message_group ")) {
                String arg = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                String id = arg.substring(0, arg.indexOf(" "));
                String message = arg.substring(arg.indexOf(" ") + 1, arg.length());
                GroupMember member = new GroupMember();
                boolean isMember = member.isMember(id, userPhoneNumber);
                if (isMember) {
                    GroupMessage sendMessage = new GroupMessage();
                    sendMessage.sendMessage(id, userPhoneNumber, message, client, group_counter_id);
                    group_counter_id++;

                } else {
                    System.out.println("access denied");
                }

            } else if (enteredCommand.startsWith("set_channel_link ") && loggedIn) {
                String arg = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                //  System.out.println(arg);
                String channelID = arg.substring(0, arg.indexOf(" "));
                // System.out.println(channelID);
                String link = arg.substring(arg.indexOf(" ") + 1, arg.length());
                //System.out.println(link);
                Channel updateLink = new Channel();
                if (updateLink.isAdmin(userPhoneNumber, channelID)) {
                    updateLink.setChannelLink(channelID, link);
                } else {
                    System.out.println("Access denied");
                }
            } else if (enteredCommand.startsWith("set_group_link ") && loggedIn) {
                String arg = enteredCommand.substring(15, enteredCommand.length());
                //System.out.println(arg);
                String id = arg.substring(0, arg.indexOf(" "));
                //System.out.println(id);
                //System.out.println(id.length());
                String link = arg.substring(arg.indexOf(" ") + 1, arg.length());
                //System.out.println(link);
                Groupes adminList = new Groupes();
                String adminNumber = adminList.findAdmin(id);
                adminList.dbConnection.connection.close();
                //System.out.println(adminNumber);
                if (adminNumber.equals(userPhoneNumber)) {
                    //System.out.println("if executed");
                    adminList.setLink(id, link);

                }

            } else if (enteredCommand.startsWith("join_channel ") && loggedIn) {
                String id = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                //System.out.println(id);
                ChannelMembers newMember = new ChannelMembers();
                newMember.addMember(userPhoneNumber, id);
            } else if (enteredCommand.startsWith("join_link ") && loggedIn) {
                String link = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                Channel findID = new Channel();
                String id = findID.findChannelByLink(link);
                if (id != null) {
                    ChannelMembers newMember = new ChannelMembers();
                    newMember.addMember(userPhoneNumber, id);
                }
                Groupes findgroupID = new Groupes();
                String grID = findgroupID.findGroupByLink(link);
                if (grID != null) {
                    GroupMember newMember = new GroupMember();
                    newMember.addMember(grID, userPhoneNumber);
                }


            } else if (enteredCommand.startsWith("leave_channel ")) {
                String id = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                ChannelMembers removeMember = new ChannelMembers();
                removeMember.removeMember(userPhoneNumber, id);

            } else if (enteredCommand.startsWith("leave_group ")) {
                String id = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                GroupMember removeMember = new GroupMember();
                removeMember.removeMember(id, userPhoneNumber);
            } else if (enteredCommand.startsWith("home")) {
                // this function will fetch 8 last chats an group them by sender and also find number of unread message


            } else if (enteredCommand.startsWith("view_chat ") && loggedIn) {
                String phoneNumber = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                PrivateChats messages = new PrivateChats();
                ResultSet resultSet = messages.fetchMessages(phoneNumber);
                while (resultSet != null && resultSet.next()) {
                    System.out.println(resultSet.getString("sender") + "\t" + resultSet.getString("receiver") +
                            "\t" + resultSet.getString("text"));
                }
                messages.dbConnection.connection.close();

            } else if (enteredCommand.startsWith("view_channel ") && loggedIn) {
                String channelID = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                ChannelMessage last20messages = new ChannelMessage();
                ResultSet resultSet = last20messages.fetchMessages(channelID);
                while (resultSet != null && resultSet.next()) {
                    System.out.println(resultSet.getString("text"));
                }
                last20messages.dbConnection.connection.close();

            } else if (enteredCommand.startsWith("view_user_profile ")) {
                String phoneNumber = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                //System.out.println(phoneNumber);
                User profile = new User(phoneNumber);
                ResultSet profileDetails = profile.viewProfile();
                while (profileDetails != null && profileDetails.next()) {
                    System.out.println(profileDetails.getString("name") + "\t" + profileDetails.getString("user_id") + "" +
                            "\t" + profileDetails.getString("link"));
                }
                profile.connection.connection.close();
            } else if (enteredCommand.startsWith("view_channel_profile ")) {
                String channelID = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                Channel channelProfile = new Channel();
                ResultSet profile = channelProfile.viewChannelProfile(channelID);

                while (profile != null && profile.next()) {
                    System.out.println(profile.getString("channel_id") + "\t" + profile.getString("name") +
                            "\t" + profile.getString("chanel_link"));
                }
                profile.absolute(1);
                //System.out.println(profile.getString("admin_phone_number"));

                if (profile.getString("admin_phone_number").equals(userPhoneNumber)) {
                    ChannelMembers membersList = new ChannelMembers();
                    ResultSet members = membersList.viewMembers(channelID);
                    while (members != null && members.next()) {
                        System.out.println(members.getString("member_id"));
                    }
                    channelProfile.dbConnection.connection.close();
                    membersList.dbConnection.connection.close();
                }
            } else if (enteredCommand.startsWith("view_group_profile ")) {

                String groupID = enteredCommand.substring(enteredCommand.indexOf(" ") + 1, enteredCommand.length());
                Groupes membersList = new Groupes();
                ResultSet resultSet = membersList.viewGroupProfile(groupID);
                while (resultSet != null && resultSet.next()) {
                    System.out.println(resultSet.getString("group_id") + "\t" + resultSet.getString("name")
                            + "\t" + resultSet.getString("link"));
                }
                membersList.dbConnection.connection.close();
            } else if (enteredCommand.startsWith("count_unread") && loggedIn) {
                PrivateChats findUnreadMessages = new PrivateChats();
                int count = findUnreadMessages.countUnreadMessages(userPhoneNumber);
                if (count != -1) {
                    System.out.println(count);
                } else {
                    System.out.println("no unread message");
                }
            } else if (enteredCommand.startsWith(("search_all")) && loggedIn) {
                Search search = new Search();
                search.searchAll(enteredCommand.substring(11), userPhoneNumber, client);
            } else if (enteredCommand.startsWith("search_chat") && loggedIn) {
                Search search = new Search();
                search.search_spec(enteredCommand.substring(24), userPhoneNumber, enteredCommand.substring(12, 23), client);
            } else if ((enteredCommand.startsWith("search_group_by_sender")) && loggedIn) {
                String forID = enteredCommand.substring(23);
                String id = forID.substring(0, forID.indexOf(" "));
                String forPhone = forID.substring(forID.indexOf(" ") + 1);
                String phone = forPhone.substring(0, forPhone.indexOf(" "));
                String text = forPhone.substring(forPhone.indexOf(" ") + 1);
                GroupMember member = new GroupMember();
                boolean isMember = member.isMember(id, userPhoneNumber);
                if (!isMember) {
                    System.out.println("access denied");
                    continue;
                }
                Search search = new Search();
                search.search_group_by_sender(id, phone, text, client);

            } else if (enteredCommand.startsWith("view_messages_by_date ") && loggedIn) {
                String date = enteredCommand.substring(22);
                Search search = new Search();
                date = date.replace('/', '-');
                search.view_messages_by_date(userPhoneNumber, date, client);
            }
            else if(enteredCommand.startsWith("view_messages_by_date_group") && loggedIn){
                String usefulString = enteredCommand.substring(28);
                int space = usefulString.indexOf(" ");
                String id = usefulString.substring(0, space);
                String date = usefulString.substring(space + 1);
                date = date.replace('/', '-');
                Search search = new Search();
                search.view_messages_by_date_group(id, date, client);
            }
            else if (enteredCommand.startsWith("search_group ") && loggedIn){
                String usefulString = enteredCommand.substring(13);
                int space = usefulString.indexOf(" ");
                String id = usefulString.substring(0, space);
                String text = usefulString.substring(space + 1);
                Search search = new Search();
                search.search_group(id, text, client);
            }
            else if (enteredCommand.startsWith("search_channel ") && loggedIn){
                String usefulString = enteredCommand.substring(15);
                int space = usefulString.indexOf(" ");
                String id = usefulString.substring(0, space);
                String text = usefulString.substring(space + 1);
                Search search = new Search();
                search.search_channel(id, text, client);
            }
            else if (enteredCommand.startsWith("search_all_fuzzy") && loggedIn){
                Search search = new Search();
                search.searchAllFuzzy(enteredCommand.substring(17), userPhoneNumber, client);
            }
            else if (enteredCommand.startsWith("view_messages_by_date_chat")){
                int space = enteredCommand.indexOf(" ");
                String usefulCommand = enteredCommand.substring(space + 1);
                space = usefulCommand.indexOf(" ");
                String phoneID = usefulCommand.substring(0, space);
                String dateTemp = usefulCommand.substring(space + 1);
                String date = dateTemp.replace('/', '-');
                Search search = new Search();
                search.view_messages_by_date_chat(userPhoneNumber, phoneID, date, client);
            }
            else if (enteredCommand.startsWith("view_messages_by_date_channel") && loggedIn){
                int space = enteredCommand.indexOf(" ");
                String usefulCommand = enteredCommand.substring(space + 1);
                space = usefulCommand.indexOf(" ");
                String id = usefulCommand.substring(0, space);
                String dateTemp = usefulCommand.substring(space + 1);
                String date = dateTemp.replace('/', '-');
                Search search = new Search();
                search.view_messages_by_date_channel(id, date, client);
            }

        }
    }
}
