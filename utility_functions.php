<?php
/**
 * Created by PhpStorm.
 * User: yonica
 * Date: 18/06/2016
 * Time: 4:12 PM
 */

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/connection.php';

function getChannel($channel_id)
{
    try {
        $stmt = getConnection()->prepare("SELECT * FROM channels WHERE channel_id = ?");
        $stmt->execute([$channel_id]);

        return $stmt->fetch(PDO::FETCH_ASSOC);
    } catch (Exception $e) {
        echo $e->getMessage() . "\n";
        return false;
    }
}

function getContactPhoneNumbers($contact_ids)
{
    try {
        $result = getConnection()->query('SELECT phone_number FROM contacts WHERE contact_id IN (' . $contact_ids . ');');
        return $result->fetchAll(PDO::FETCH_ASSOC);
    } catch (Exception $e) {
        echo $e->getMessage() . "\n";
        return false;
    }
}

function setContactValidity($contact_phone_number, $validity)
{
    try {
        $stmt = getConnection()->prepare("UPDATE contacts SET status = ?, valid = ? WHERE phone_number = ?");
        $stmt->execute(["RESOLVED", $validity, $contact_phone_number]);
    } catch (Exception $e) {
        echo $e->getMessage() . "\n";
    }
}

function markChannelAsBlocked($channel_id)
{
    try {
        $stmt = getConnection()->prepare("UPDATE channels SET valid = ? WHERE channel_id = ?");
        $stmt->execute([FALSE, $channel_id]);
    } catch (Exception $e) {
        echo $e->getMessage() . "\n";
    }
}

function markContactAsValid($contact_phone_number)
{
    setContactsValidity($contact_phone_number, true);
}

function markContactAsInvalid($contact_phone_number)
{
    setContactsValidity($contact_phone_number, false);
}

function associateContactsToCampaign($campaign_id, $contact_ids)
{
    global $DBH;

    if (!isset($campaign_id) || !isset($contact_ids)) {
        syslog(LOG_ERR, 'Could not associate contacts to campaign, data is corrupted');

        return;
    }

    $contact_ids = join(',', $contact_ids);

    $all_contacts_stmt = $DBH->query('SELECT * FROM contacts WHERE id IN (' . $contact_ids . ');');

    $all_contacts_result = $all_contacts_stmt->fetchAll(PDO::FETCH_ASSOC);

    if ($all_contacts_result) {
        foreach ($all_contacts_result as $contact_data) {
            $add_operation_contact_stmt = $DBH->prepare('INSERT IGNORE INTO operation_contacts (id, campaign_id, valid, status) VALUES (:contact_id, :campaign_id, :contact_valid, :contact_status);');
            $add_operation_contact_stmt->execute(array(':contact_id' => $contact_data['id'], ':campaign_id' => $campaign_id, ':contact_valid' => $contact_data['valid'], ':contact_status' => $contact_data['status']));
        }
    } else {
        syslog(LOG_DEBUG, 'Contacts does not exist, therefore could not associate them to the campaign ' . $campaign_id);
    }
}

function unassociateContactsFromCampaign($campaign_id, $contact_ids = null, $unassociate_all = FALSE)
{
    global $DBH;

    $DBH->beginTransaction();

    if (isset($campaign_id)) {
        if (isset($contact_ids)) {
            $contact_ids = join(',', $contact_ids);

            $remove_operation_contact_stmt = $DBH->prepare('DELETE FROM operation_contacts WHERE campaign_id = :campaign_id AND id IN (' . $contact_ids . ');');
            $remove_operation_contact_stmt->execute(array(':campaign_id' => $campaign_id));
        } else if ($unassociate_all) {
            $remove_all_operation_contacts_stmt = $DBH->prepare('DELETE FROM operation_contacts WHERE campaign_id = :campaign_id;');
            $remove_all_operation_contacts_stmt->execute(array(':campaign_id' => $campaign_id));
        }
    }

    $DBH->commit();
}

function setContactsActiveState($campaign_id, $contact_ids, $active_state)
{
    global $DBH;

    if (!isset($campaign_id) || !isset($contact_ids) || !isset($active_state)) {
        syslog(LOG_ERR, 'Could not set active state ' . $active_state . ' for contacts in campaign ' . $campaign_id . ', data is corrupted');

        return;
    }

    $DBH->beginTransaction();

    $contact_ids = join(',', $contact_ids);

    $update_operation_contact__active_state_stmt = $DBH->prepare('UPDATE operation_contacts SET active = :active_state WHERE campaign_id = :campaign_id AND id IN (' . $contact_ids . ');');
    $update_operation_contact__active_state_stmt->execute(array(':active_state' => $active_state, ':campaign_id' => $campaign_id));

    $DBH->commit();
}

function findUnresolvedContacts($campaign_id)
{
    global $DBH, $config;

    $find_unresolved_contacts_stmt = $DBH->prepare('SELECT * FROM operation_contacts WHERE campaign_id = :campaign_id AND active IS TRUE AND valid IS FALSE AND status = :unresolved_status AND channel_id IS NULL AND targeted IS FALSE;');
    $find_unresolved_contacts_stmt->execute(array(':campaign_id' => $campaign_id, ':unresolved_status' => $config['CONTACT_STATUS']['UNRESOLVED']));

    return $find_unresolved_contacts_stmt->fetchAll(PDO::FETCH_ASSOC);
}

function findUntrackedContacts($campaign_id)
{
    global $DBH, $config;

    if (!isset($campaign_id)) {
        syslog(LOG_ERR, 'Could not find untracked contacts, data is corrupted');

        return [];
    }

    $find_untracked_contacts_stmt = $DBH->prepare('SELECT * FROM operation_contacts WHERE campaign_id = :campaign_id AND active IS TRUE AND valid IS TRUE AND status = :resolved_status AND channel_id IS NULL AND targeted IS FALSE;');
    $find_untracked_contacts_stmt->execute(array(':campaign_id' => $campaign_id, ':resolved_status' => $config['CONTACT_STATUS']['RESOLVED']));

    return $find_untracked_contacts_stmt->fetchAll(PDO::FETCH_ASSOC);
}

function findAssignedButInactiveContacts($campaign_id)
{
    // Shouldn't happen but it's an edge case we should support
    // Can be both contacts for tracking & resolving
    global $DBH;

    $find_assigned_but_inactive_contacts_stmt = $DBH->prepare('SELECT * FROM operation_contacts WHERE campaign_id = :campaign_id AND active IS TRUE AND channel_id IS NOT NULL AND targeted IS FALSE;');
    $find_assigned_but_inactive_contacts_stmt->execute(array(':campaign_id' => $campaign_id));

    return $find_assigned_but_inactive_contacts_stmt->fetchAll(PDO::FETCH_ASSOC);
}

function assignChannelsForOperation($campaign_id, $operation, $contact_ids, $channel_id_to_contact_ids_map = null)
{
    global $DBH, $config;

    if (!isset($channel_id_to_contact_ids_map)) {
        $channel_id_to_contact_ids_map = array();
    }

    if (!isset($campaign_id) || !isset($operation) || !isset($contact_ids) || count($contact_ids) === 0) {
        return $channel_id_to_contact_ids_map;
    }

    $existing_available_channel_stmt = $DBH->prepare('SELECT * FROM channels WHERE valid IS TRUE AND operation = :operation AND used_contacts_amount > 0 AND used_contacts_amount < :operation_contacts_limit LIMIT 1;');
    $existing_available_channel_stmt->execute(array(':operation' => $operation, ':operation_contacts_limit' => $config['OPERATION_TO_CONTACTS_LIMIT'][$operation]));

    $find_channel_result = $existing_available_channel_stmt->fetch(PDO::FETCH_ASSOC);
    // If there's no vacant channel we'll need to assign a new one (if possible)
    if ($find_channel_result === false) {
        // Assign new channel for this campaign and set all the contacts for tracking
        $new_channel_stmt = $DBH->query('SELECT * FROM channels WHERE valid IS TRUE AND used_contacts_amount = 0 LIMIT 1;');
        $new_channel_result = $new_channel_stmt->fetch(PDO::FETCH_ASSOC);

        if ($new_channel_result) {
            $chosen_channel = $new_channel_result;
        } else {
            echo 'Could not find new channel for assignment to campaign ' . $campaign_id . "</br>";
            return $channel_id_to_contact_ids_map;
        }
    } else {
        $chosen_channel = $find_channel_result;
    }

    // Use this offset to fill the required gap until the channel is fully loaded with tracked contacts
    $chunk_offset = $config['OPERATION_TO_CONTACTS_LIMIT'][$operation] - $chosen_channel['used_contacts_amount'];

    $contacts_chunk = array_slice($contact_ids, 0, $chunk_offset);

    // If a channel has been unassigned during the recursion and can take more contacts
    if (isset($channel_id_to_contact_ids_map[$chosen_channel['id']])) {
        $channel_id_to_contact_ids_map[$chosen_channel['id']] = array_merge($channel_id_to_contact_ids_map[$chosen_channel['id']], $contacts_chunk);
    } else {
        $channel_id_to_contact_ids_map[$chosen_channel['id']] = $contacts_chunk;
    }

    assignContactsToChannel($chosen_channel, $campaign_id, $operation, $contacts_chunk);

    $contact_ids = array_slice($contact_ids, $chunk_offset);

    return assignChannelsForOperation($campaign_id, $operation, $contact_ids, $channel_id_to_contact_ids_map);
}

function assignContactsToChannel($channel, $campaign_id, $operation, $contact_ids)
{
    global $DBH, $config;

    $DBH->beginTransaction();

    // Update channel with the new amount of tracked contacts
    $update_channel_stmt = $DBH->prepare('UPDATE channels SET used_contacts_amount = :updated_used_contacts_amount, operation = :operation WHERE id = :channel_id;');
    $update_channel_stmt->execute(array(':updated_used_contacts_amount' => ($channel['used_contacts_amount'] + count($contact_ids)), ':operation' => $operation, ':channel_id' => $channel['id']));

    $contact_ids = join(',', $contact_ids);

    if ($operation === $config['OPERATIONS']['RESOLVING']) {
        // Update operation contacts with resolving operation
        $update_operation_contacts_stmt = $DBH->prepare('UPDATE operation_contacts SET channel_id = :channel_id, status = :resolving_status WHERE campaign_id = :campaign_id AND id IN (' . $contact_ids . ');');
        $update_operation_contacts_stmt->execute(array(':channel_id' => $channel['id'], ':campaign_id' => $campaign_id, ':resolving_status' => $config['CONTACT_STATUS']['RESOLVING']));

        // Update global contacts with resolving operation (so the background resolver won't resolve the same contact)
        $update_operation_contacts_stmt = $DBH->prepare('UPDATE contacts SET status = :resolving_status WHERE id IN (' . $contact_ids . ');');
        $update_operation_contacts_stmt->execute(array(':resolving_status' => $config['CONTACT_STATUS']['RESOLVING']));
    } else {
        $update_contacts_stmt = $DBH->prepare('UPDATE operation_contacts SET channel_id = :channel_id WHERE campaign_id = :campaign_id AND id IN (' . $contact_ids . ');');
        $update_contacts_stmt->execute(array(':channel_id' => $channel['id'], ':campaign_id' => $campaign_id));

    }

    $DBH->commit();
}

function assignContactsOffChannel($channel, $campaign_id, $contact_ids)
{
    global $DBH;

    $DBH->beginTransaction();

    if ($channel['used_contacts_amount'] - count($contact_ids) === 0) {
        // Update channel with the new amount of tracked contacts
        $update_channel_stmt = $DBH->prepare('UPDATE channels SET used_contacts_amount = :updated_used_contacts_amount, operation = NULL WHERE id = :channel_id;');
        $update_channel_stmt->execute(array(':updated_used_contacts_amount' => 0, ':channel_id' => $channel['id']));
    } else {
        // Update channel with the new amount of tracked contacts
        $update_channel_stmt = $DBH->prepare('UPDATE channels SET used_contacts_amount = :updated_used_contacts_amount WHERE id = :channel_id;');
        $update_channel_stmt->execute(array(':updated_used_contacts_amount' => ($channel['used_contacts_amount'] - count($contact_ids)), ':channel_id' => $channel['id']));
    }

    $contact_ids = join(',', $contact_ids);

    $update_contacts_stmt = $DBH->prepare('UPDATE operation_contacts SET channel_id = NULL WHERE campaign_id = :campaign_id AND id IN (' . $contact_ids . ');');
    $update_contacts_stmt->execute(array(':campaign_id' => $campaign_id));

    $DBH->commit();
}

function extractIdsFromContacts($contacts_data)
{
    if (!isset($contacts_data) || count($contacts_data) === 0) {
        return [];
    }

    return __::flatten(__::pluck($contacts_data, 'id'));
}

function setContactsInActionState($contact_ids, $in_action_state)
{
    global $DBH;

    $binded_contact_ids = join(',', $contact_ids);

    $mark_contacts_in_action_state_stmt = $DBH->query('UPDATE operation_contacts SET in_action = :in_action_state WHERE id IN (' . $binded_contact_ids . ');');
    $mark_contacts_in_action_state_stmt->execute(array(':in_action_state' => $in_action_state));
}

function fetchUnresolvedAndNonOperationalContacts()
{
    global $DBH, $config;

    $unresolved_contacts_stmt = $DBH->prepare('SELECT id FROM contacts WHERE status = :unresolved_status AND id NOT IN (SELECT DISTINCT(id) FROM operation_contacts);');
    $unresolved_contacts_stmt->execute(array(':unresolved_status' => $config['CONTACT_STATUS']['UNRESOLVED']));

    return $unresolved_contacts_stmt->fetchAll(PDO::FETCH_ASSOC);
}

function setContactsValidity(array $contact_ids, $is_valid)
{
    global $DBH;

    $binded_contact_ids = join(',', $contact_ids);

    $DBH->beginTransaction();

    // Update global contact
    $set_contacts_validity = $DBH->prepare('UPDATE contacts SET valid = :is_valid WHERE id IN (' . $binded_contact_ids . ');');
    $set_contacts_validity->execute(array(':is_valid' => $is_valid));

    // Update every operational occurence of that contact
    $set_operational_contacts_validity = $DBH->prepare('UPDATE operation_contacts SET valid = :is_valid WHERE id IN (' . $binded_contact_ids . ');');
    $set_operational_contacts_validity->execute(array(':is_valid' => $is_valid));

    $DBH->commit();
}

?>