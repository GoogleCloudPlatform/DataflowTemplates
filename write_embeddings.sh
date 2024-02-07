set -x
. ./env.sh

#
# ----------------------------------------
# row_key_0
#   cf:allow                                 @ 2024/02/07-19:53:16.137000
#     "thing 1"
#   cf:crowding_tag                          @ 2024/02/07-19:53:16.137000
#     "a"
#   cf:deny                                  @ 2024/02/07-19:53:16.137000
#     "thing 2"
#   cf:double                                @ 2024/02/07-19:53:16.137000
#     "@\x05\xae\x14z\xe1G\xae"
#   cf:embeddings                            @ 2024/02/07-19:53:16.137000
#   cf:float                                 @ 2024/02/07-19:53:16.137000
#     "@H\xf5\xc3"
#   cf:int                                   @ 2024/02/07-19:53:16.137000
#     "\x00\x00\xaf\xc8"
#
EMBEDDING="0x3ddde8883f23f6723f48c8203e81d2ef3ed7aac23e5a1ddd3f7a9eb93f1f6c773f00027e3f6cc7843f12a7363f41dcdb3e39c0003eea87c33f2775083e3134873f535c7d3f4675873db7e2923f6a7f743f07bc5f3f1debb33f6381533cc81e7b3f024c6d3eea43213eef131b3f4fccce3f6a95c53de27a413f3a9d863f4f1d9b3f7adb263dec9c023e865a6f3d82a28e3f34afd93e8d5b7c3f09c4283e8d7a303f62b4be3edd58b33f7082a73f2e649c3d1a95d13f4303a23eb81df63e93a5173f57f7013db54eb73f5393aa3e842bd73f7424b03ef267773e10e3553f267efe3ef8a9b53e3754203eca4d9b3f5587c93ef80d973e91af923e7a90663d55b9a43f243e5a3e51f6193f12599b3ec09af03f4391e73f6001a03eaf56b23eb42fc13ef8ad1b3f6891e43e3871f83d54bb173dc884303f08e0683ebfdcde3f7e33e33efa32bd3ed55ee43dd4477e3f418ce13f7d9cd53f18c50d3e1ece893ee83bcb3f552d5c3f798b003ebac9cf3e9bd7f83ea929b23ef7fdbb3ee3567f3e8469693ebf8a0c3f61048c3f39d36e3f1c0c413f71ac183e2469b13f3ac5853eacffe53e191e5a3f7e6dc63ebf8f4d3e2d260d3f0784323e9d514c3e7212593f2af0853e6e35243f016f5f3eada95b3f46e9343f053b493eb26a893e4321823f5e77b53e8c05123f7119a23d28c69b3f63a1973ef995643ef3943a3e9f32973f362bc7"

#~/go/bin/cbt -project $PROJECT -instance $INSTANCE set $TABLE foo-key cf1:float="0x4048f5c3"
~/go/bin/cbt -project $PROJECT -instance $INSTANCE set $TABLE foo-key cf1:embeddings="$EMBEDDING" cf1:float="0x4048f5c3" cf1:int="0x0000afc8" cf1:double="0x4005ae147ae147ae" cf1:allow="thing 1" cf1:crowding_tag="a" cf1:deny="thing 2"
