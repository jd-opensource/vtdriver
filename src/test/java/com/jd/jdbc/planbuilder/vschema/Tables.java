/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.planbuilder.vschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Tables {

    @JsonProperty("music_extra")
    private MusicExtra musicExtra;

    @JsonProperty("samecolvin")
    private Samecolvin samecolvin;

    @JsonProperty("multicolvin")
    private Multicolvin multicolvin;

    @JsonProperty("user_metadata")
    private UserMetadata userMetadata;

    @JsonProperty("user_extra")
    private UserExtra userExtra;

    @JsonProperty("music")
    private Music music;

    @JsonProperty("authoritative")
    private Authoritative authoritative;

    @JsonProperty("pin_test")
    private PinTest pinTest;

    @JsonProperty("overlap_vindex")
    private OverlapVindex overlapVindex;

    @JsonProperty("weird`name")
    private WeirdName weirdName;

    @JsonProperty("user")
    private User user;

    @JsonProperty("seq")
    private Seq seq;

    @JsonProperty("dual")
    private Dual dual;

    @JsonProperty("m1")
    private M1 m1;

    @JsonProperty("unsharded_b")
    private UnshardedB unshardedB;

    @JsonProperty("foo")
    private Foo foo;

    @JsonProperty("unsharded_a")
    private UnshardedA unshardedA;

    @JsonProperty("unsharded_authoritative")
    private UnshardedAuthoritative unshardedAuthoritative;

    @JsonProperty("unsharded")
    private Unsharded unsharded;

    @JsonProperty("unsharded_auto")
    private UnshardedAuto unshardedAuto;

    @JsonProperty("test")
    private Test test;

    public MusicExtra getMusicExtra() {
        return musicExtra;
    }

    public Samecolvin getSamecolvin() {
        return samecolvin;
    }

    public Multicolvin getMulticolvin() {
        return multicolvin;
    }

    public UserMetadata getUserMetadata() {
        return userMetadata;
    }

    public UserExtra getUserExtra() {
        return userExtra;
    }

    public Music getMusic() {
        return music;
    }

    public Authoritative getAuthoritative() {
        return authoritative;
    }

    public PinTest getPinTest() {
        return pinTest;
    }

    public OverlapVindex getOverlapVindex() {
        return overlapVindex;
    }

    public WeirdName getWeirdName() {
        return weirdName;
    }

    public User getUser() {
        return user;
    }

    public Seq getSeq() {
        return seq;
    }

    public Dual getDual() {
        return dual;
    }

    public M1 getM1() {
        return m1;
    }

    public UnshardedB getUnshardedB() {
        return unshardedB;
    }

    public Foo getFoo() {
        return foo;
    }

    public UnshardedA getUnshardedA() {
        return unshardedA;
    }

    public UnshardedAuthoritative getUnshardedAuthoritative() {
        return unshardedAuthoritative;
    }

    public Unsharded getUnsharded() {
        return unsharded;
    }

    public UnshardedAuto getUnshardedAuto() {
        return unshardedAuto;
    }

    public Test getTest() {
        return test;
    }
}