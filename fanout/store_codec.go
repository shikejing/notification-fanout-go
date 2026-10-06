package fanout

func toJSON(e Entry) jsonEntry {
	return jsonEntry{
		Seq:        e.Seq,
		Kind:       e.Kind,
		Time:       e.Time,
		EventID:    e.EventID,
		DeliveryID: e.DeliveryID,
		Channel:    e.Channel,
		AttemptNo:  e.AttemptNo,
		Starting:   e.Starting,
		Status:     e.Status,
		Reason:     e.Reason,
		NotBefore:  e.NotBefore,
		Payload:    e.Payload,
	}
}

func fromJSON(je jsonEntry) Entry {
	return Entry{
		Seq:        je.Seq,
		Kind:       je.Kind,
		Time:       je.Time,
		EventID:    je.EventID,
		DeliveryID: je.DeliveryID,
		Channel:    je.Channel,
		AttemptNo:  je.AttemptNo,
		Starting:   je.Starting,
		Status:     je.Status,
		Reason:     je.Reason,
		NotBefore:  je.NotBefore,
		Payload:    je.Payload,
	}
}
