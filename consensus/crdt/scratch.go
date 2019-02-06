package crdt

// // Element represents a set element
// type Element struct {
// 	ID    cid.Cid
// 	Block cid.Cid
// 	Pin   api.Pin
// }

// func (e *Element) dsKey() ds.Key {
// 	return dshelp.CidToDsKey(e.ID).Child(dshelp.CidToDsKey(e.Block))
// }

// func (e *Element) proto() *pb.Element {
// 	return &pb.Element{
// 		Cid:       e.ID.String(),
// 		Rmin:      e.Pin.ReplicationFactorMin,
// 		Rmax:      e.Pin.ReplicationFactorMax,
// 		Name:      e.Pin.Name,
// 		ShardSize: e.Pin.ShardSize,
// 	}
// }

// func (e *Element) fromProto(pbuf *pb.Element) error {
// 	ci, err := cid.Decode(pbElem.Cid)
// 	if err != nil {
// 		return err
// 	}
// 	e.Cid = ci
// 	e.Pin = api.PinWithOpts(
// 		ci,
// 		api.PinOptions{
// 			ReplicationFactorMin: pbElem.Rmin,
// 			ReplicationFactorMax: pbElem.Rmax,
// 			Name:                 pbElem.Name,
// 			ShardSize:            pbElem.ShardSize,
// 		},
// 	)
// }

// // MarshalBinary serializes an Element.
// func (e *Element) MarshalBinary() ([]byte, error) {
// 	return proto.Marshal(e.proto())
// }

// // UnmarshalBinary deserializes an Element.
// func (e *Element) UnmarshalBinary(data []byte) error {
// 	pbElem := &pb.Element{}
// 	err := proto.Unmarshal(data, pbElem)
// 	if err != nil {
// 		return err
// 	}
// 	return e.fromProto(pbElem)
// }

// // Delta represents a OR-Set CRDT delta.
// type Delta struct {
// 	Elements   []*Element
// 	Tombstones []*Element
// }

// func (d *Delta) MarshalBinary() ([]byte, error) {
// 	pbDelta := &pb.Delta{}
// 	for _, e := range d.Elements {
// 		pbDelta.Elements = append(pbDelta.Elements, e.proto())
// 	}
// 	for _, t := range d.Tombstones {
// 		pbDelta.Tombstones = append(pbDelta.Tombstones, t.proto())
// 	}
// }

// func (d *Delta) UnmarshalBinary(data []byte) error {

// }
