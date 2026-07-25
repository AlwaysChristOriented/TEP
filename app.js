  let bookmarkIds = new Set();
  let allBibleVerses = [];
  let allSourceRows = [];

  // Fuse.js search runs across a small pool of background workers so it can
  // never block the main thread. It's a pool rather than a single worker
  // because the app now auto-loads ~30 separate source-text collections
  // (Qur'an, six Hadith books, eight Hindu texts including the 15MB
  // Mahabharata, eight Buddhist texts, six Catholic/Apocrypha texts) — a
  // single worker searching all of them one after another is what made an
  // "All traditions" query take several seconds once enough texts were
  // loaded. Splitting the ~30 source-text collections across N workers and
  // searching them concurrently cuts that to roughly 1/N of the wall time
  // for the same results (see loadSourceIntoWorkers / handlePoolMessage).
  const SEARCH_POOL_SIZE = Math.min(6, Math.max(2, (navigator.hardwareConcurrency || 4)));
  const MERGE_TOP_CAP = 30; // matches search-worker.js's own TOP_CAP
  let searchWorkers = [];
  let bibleIndexReady = false;
  let sourceIndexReady = false;
  let searchReqSeq = 0;
  let latestSearchReqId = 0;
  const EMPTY_MATCHES = { total: 0, top: [] };
  let pendingOriginalSource = EMPTY_MATCHES;

  function initSearchWorkers() {
    if (typeof Worker === 'undefined') return; // no Worker support — search panels degrade to empty, non-fatal
    try {
      searchWorkers = Array.from({ length: SEARCH_POOL_SIZE }, (_, i) => {
        const w = new Worker('./search-worker.js');
        w.onmessage = (e) => handlePoolMessage(i, e);
        w.onerror = (err) => handlePoolWorkerError(i, err);
        return w;
      });
    } catch (err) {
      // A Worker() constructor failure here (restrictive CSP, resource limits,
      // some embedded-webview quirk) must never take the rest of app.js down
      // with it — this call runs before everything else in the file, so an
      // uncaught throw here would silently break the whole app, not just
      // search. Degrade the same way "no Worker support" already does.
      console.warn('TEP: failed to start search workers, search will be unavailable:', err);
      searchWorkers = [];
    }
  }
  initSearchWorkers();

  const ENTRIES = [
    // NOTE: entries marked "VERIFY WORDING" quote from a non-Bible source text
    // where exact translation/edition wording should be checked against the
    // primary source before publishing.
    {
      claim: "Jesus was just a prophet, not God",
      religion: "Islam",
      keywords: ["jesus is just a prophet", "jesus a prophet", "muhammad greater than jesus", "jesus not divine islam", "isa prophet"],
      sourceQuote: { ref: "Qur'an 4:171", text: "Christ Jesus the son of Mary was (no more than) a messenger of Allah." }, // VERIFY WORDING
      note: "The Qur'an affirms Jesus as a prophet and messenger, but explicitly denies his divinity and sonship.",
      verses: [
        { ref: "John 1:1", text: "In the beginning was the Word, and the Word was with God, and the Word was God." },
        { ref: "John 8:58", text: "Jesus said unto them, Verily, verily, I say unto you, Before Abraham was, I am." },
        { ref: "Colossians 2:9", text: "For in him dwelleth all the fulness of the Godhead bodily." }
      ]
    },
    {
      claim: "Jesus wasn't actually crucified",
      religion: "Islam",
      keywords: ["jesus not crucified", "islam crucifixion", "made to appear", "swoon theory islam"],
      sourceQuote: { ref: "Qur'an 4:157", text: "...they killed him not, nor crucified him, but so it was made to appear unto them..." }, // VERIFY WORDING
      note: "Most Islamic scholars read this as denying the crucifixion happened at all, holding that Jesus was raised to heaven before it.",
      verses: [
        { ref: "1 Corinthians 15:3-4", text: "For I delivered unto you first of all that which I also received, how that Christ died for our sins according to the scriptures; And that he was buried, and that he rose again the third day according to the scriptures:" },
        { ref: "John 19:33-34", text: "But when they came to Jesus, and saw that he was dead already, they brake not his legs: But one of the soldiers with a spear pierced his side, and forthwith came there out blood and water." }
      ]
    },
    {
      claim: "Salvation on Judgment Day depends on whether one's good deeds outweigh the bad on the scales",
      religion: "Islam",
      keywords: ["scales of deeds islam", "weighing of good and bad deeds", "mizan judgment day", "islam salvation by works"],
      sourceQuote: { ref: "Qur'an 23:102-103", text: "Then those whose balance (of good deeds) is heavy,- they will attain salvation: But those whose balance is light, will be those who have lost their souls, in Hell will they abide." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" },
        { ref: "Galatians 2:16", text: "Knowing that a man is not justified by the works of the law, but by the faith of Jesus Christ, even we have believed in Jesus Christ, that we might be justified by the faith of Christ, and not by the works of the law: for by the works of the law shall no flesh be justified." }
      ]
    },
    {
      claim: "The Torah and Gospel were corrupted by their followers, so the Qur'an corrects and supersedes them",
      religion: "Islam",
      keywords: ["tahrif corruption of scripture", "quran corrects the bible", "islam bible corrupted", "quran supersedes torah gospel"],
      sourceQuote: { ref: "Qur'an 5:13", text: "But because of their breach of their covenant, We cursed them, and made their hearts grow hard; they change the words from their (right) places and forget a good part of the message that was sent them..." },
      note: "This is the doctrine of tahrif (scriptural corruption), commonly invoked to explain differences between the Qur'an and the Bible.",
      verses: [
        { ref: "Psalm 119:89", text: "For ever, O LORD, thy word is settled in heaven." },
        { ref: "Matthew 24:35", text: "Heaven and earth shall pass away, but my words shall not pass away." },
        { ref: "1 Peter 1:25", text: "But the word of the Lord endureth for ever. And this is the word which by the gospel is preached unto you." }
      ]
    },
    {
      claim: "Jesus is Michael the Archangel, a created being",
      religion: "Jehovah's Witnesses",
      keywords: ["jesus is michael the archangel", "jehovah witness jesus created", "jesus not god jw", "arian jw"],
      positionSummary: "Official Jehovah's Witness teaching holds that Jesus pre-existed as the archangel Michael, God's first and greatest creation, rather than being eternally God himself.",
      verses: [
        { ref: "John 1:1", text: "In the beginning was the Word, and the Word was with God, and the Word was God." },
        { ref: "John 1:3", text: "All things were made by him; and without him was not any thing made that was made." },
        { ref: "Colossians 1:16", text: "For by him were all things created, that are in heaven, and that are in earth, visible and invisible, whether they be thrones, or dominions, or principalities, or powers: all things were created by him, and for him:" }
      ]
    },
    {
      claim: "Only 144,000 people go to heaven",
      religion: "Jehovah's Witnesses",
      keywords: ["144000", "144,000 heaven", "jehovah witness heaven", "anointed class"],
      positionSummary: "Jehovah's Witnesses teach that only a literal 144,000 \"anointed\" believers go to heaven to reign with Christ, while the rest of the faithful hope for eternal life on a paradise earth.",
      verses: [
        { ref: "Revelation 7:9", text: "After this I beheld, and, lo, a great multitude, which no man could number, of all nations, and kindreds, and people, and tongues, stood before the throne, and before the Lamb, clothed with white robes, and palms in their hands;" },
        { ref: "Revelation 7:10", text: "And cried with a loud voice, saying, Salvation to our God which sitteth upon the throne, and unto the Lamb." },
        { ref: "John 14:2-3", text: "In my Father's house are many mansions: if it were not so, I would have told you. I go to prepare a place for you. And if I go and prepare a place for you, I will come again, and receive you unto myself; that where I am, there ye may be also." }
      ]
    },
    {
      claim: "The wicked will be annihilated, not tormented forever in hell",
      religion: "Jehovah's Witnesses",
      keywords: ["annihilationism jw", "hell is the grave", "jehovah witness no eternal torment", "wicked cease to exist"],
      positionSummary: "Jehovah's Witnesses teach that hell (Sheol/Hades) is simply mankind's common grave, a state of unconscious non-existence, and that the unrepentant wicked will ultimately be annihilated — ceasing to exist entirely — rather than suffering conscious eternal torment.",
      verses: [
        { ref: "Matthew 25:46", text: "And these shall go away into everlasting punishment: but the righteous into life eternal." },
        { ref: "Revelation 14:11", text: "And the smoke of their torment ascendeth up for ever and ever: and they have no rest day nor night, who worship the beast and his image, and whosoever receiveth the mark of his name." },
        { ref: "Revelation 20:10", text: "And the devil that deceived them was cast into the lake of fire and brimstone... and shall be tormented day and night for ever and ever." }
      ]
    },
    {
      claim: "Christ returned invisibly in 1914 to begin ruling as heavenly king",
      religion: "Jehovah's Witnesses",
      keywords: ["1914 invisible return jw", "jehovah witness christ already returned", "last days began 1914", "invisible presence parousia"],
      positionSummary: "Jehovah's Witnesses teach that Jesus Christ began an invisible heavenly rule as King in the year 1914, marking the start of the \"last days,\" rather than a still-future, visible, bodily return witnessed by all people.",
      verses: [
        { ref: "Matthew 24:27", text: "For as the lightning cometh out of the east, and shineth even unto the west; so shall also the coming of the Son of man be." },
        { ref: "Acts 1:11", text: "Which also said, Ye men of Galilee, why stand ye gazing up into heaven? this same Jesus, which is taken up from you into heaven, shall so come in like manner as ye have seen him go into heaven." },
        { ref: "Revelation 1:7", text: "Behold, he cometh with clouds; and every eye shall see him, and they also which pierced him: and all kindreds of the earth shall wail because of him." }
      ]
    },
    {
      claim: "God the Father has a physical body",
      religion: "Mormonism",
      keywords: ["god has a body mormon", "heavenly father physical body", "lds god embodied"],
      sourceQuote: { ref: "Doctrine and Covenants 130:22", text: "The Father has a body of flesh and bones as tangible as man's; the Son also; but the Holy Ghost has not a body of flesh and bones, but is a personage of Spirit." }, // VERIFY WORDING
      verses: [
        { ref: "John 4:24", text: "God is a Spirit: and they that worship him must worship him in spirit and in truth." },
        { ref: "Luke 24:39", text: "Behold my hands and my feet, that it is I myself: handle me, and see; for a spirit hath not flesh and bones, as ye see me have." }
      ]
    },
    {
      claim: "The Book of Mormon is a second testament alongside the Bible",
      religion: "Mormonism",
      keywords: ["book of mormon another testament", "lds scripture book of mormon", "additional scripture mormon"],
      sourceQuote: { ref: "2 Nephi 25:23", text: "For we labor diligently to write, to persuade our children, and also our brethren, to believe in Christ, and to be reconciled to God; for we know that it is by grace that we are saved, after all we can do." }, // VERIFY WORDING
      note: "Latter-day Saints hold the Book of Mormon as scripture alongside the Bible, both understood as testaments of Jesus Christ.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Galatians 1:8-9", text: "But though we, or an angel from heaven, preach any other gospel unto you than that which we have preached unto you, let him be accursed. As we said before, so say I now again, If any man preach any other gospel unto you than that ye have received, let him be accursed." }
      ]
    },
    {
      claim: "God the Father was once a mortal man who progressed to become God, and righteous humans can likewise become gods",
      religion: "Mormonism",
      keywords: ["eternal progression mormon", "as man is god once was", "lds exaltation to godhood", "god was once a man"],
      sourceQuote: { ref: "Doctrine and Covenants 132:20", text: "Then shall they be gods, because they have no end; therefore shall they be from everlasting to everlasting, because they continue; then shall they be above all, because all things are subject unto them." }, // VERIFY WORDING
      note: "Popularly summarized in the Lorenzo Snow couplet: \"As man now is, God once was; as God now is, man may become.\"",
      verses: [
        { ref: "Isaiah 43:10", text: "...before me there was no God formed, neither shall there be after me." },
        { ref: "Malachi 3:6", text: "For I am the LORD, I change not; therefore ye sons of Jacob are not consumed." },
        { ref: "Numbers 23:19", text: "God is not a man, that he should lie; neither the son of man, that he should repent: hath he said, and shall he not do it? or hath he spoken, and shall he not make it good?" }
      ]
    },
    {
      claim: "Living people can be baptized on behalf of the dead so the deceased may receive the ordinance vicariously",
      religion: "Mormonism",
      keywords: ["baptism for the dead mormon", "proxy baptism lds", "vicarious baptism dead", "temple baptism ancestors"],
      positionSummary: "Latter-day Saints practice baptism by proxy on behalf of deceased ancestors, citing 1 Corinthians 15:29 as scriptural precedent, so that the dead may still accept the ordinance in the spirit world.",
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "John 3:36", text: "He that believeth on the Son hath everlasting life: and he that believeth not the Son shall not see life; but the wrath of God abideth on him." },
        { ref: "Hebrews 9:12", text: "Neither by the blood of goats and calves, but by his own blood he entered in once into the holy place, having obtained eternal redemption for us." }
      ]
    },
    {
      claim: "All paths lead to the same God",
      religion: "Hinduism",
      keywords: ["all religions same god", "all paths lead to god", "hindu pluralism", "many paths one god"],
      sourceQuote: { ref: "Bhagavad Gita 4:11", text: "As men approach me, so I receive them; all paths lead to me." }, // VERIFY WORDING — varies significantly by translation
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." }
      ]
    },
    {
      claim: "The soul is reincarnated again and again based on karma",
      religion: "Hinduism",
      keywords: ["reincarnation", "karma rebirth", "hindu soul next life", "samsara"],
      sourceQuote: { ref: "Bhagavad Gita 2:22", text: "As a man casts off worn-out garments and puts on others that are new, so the embodied soul casts off worn-out bodies and enters into others that are new." }, // VERIFY WORDING
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" }
      ]
    },
    {
      claim: "The four castes originate from a divine ordinance, and one's duty is fixed by the caste into which they are born",
      religion: "Hinduism",
      keywords: ["caste system hindu", "varna dharma", "four castes gita", "hereditary duty caste"],
      sourceQuote: { ref: "Bhagavad Gita 4:13", text: "The system of four stations was created by Me, according to the distinction of Gunas and Karma. Though I am their creator, know Me to be non-agent and immutable." },
      note: "Gita 18:47 reinforces this: \"Better is one's own duty, though ill done, than the duty of another, though well-performed.\"",
      verses: [
        { ref: "Galatians 3:28", text: "There is neither Jew nor Greek, there is neither bond nor free, there is neither male nor female: for ye are all one in Christ Jesus." },
        { ref: "Acts 17:26", text: "And hath made of one blood all nations of men for to dwell on all the face of the earth, and hath determined the times before appointed, and the bounds of their habitation;" }
      ]
    },
    {
      claim: "The individual self (atman) is ultimately identical with the ultimate reality (Brahman)",
      religion: "Hinduism",
      keywords: ["atman is brahman", "tat tvam asi", "that art thou upanishad", "hindu monism self is god"],
      sourceQuote: { ref: "Chandogya Upanishad 440", text: "...this whole world has that as its soul. That is Reality. That is Atman (Soul). That art thou, Svetaketu." },
      verses: [
        { ref: "Isaiah 55:8-9", text: "For my thoughts are not your thoughts, neither are your ways my ways, saith the LORD. For as the heavens are higher than the earth, so are my ways higher than your ways, and my thoughts than your thoughts." },
        { ref: "Isaiah 46:9", text: "...I am God, and there is none else; I am God, and there is none like me," }
      ]
    },
    {
      claim: "Suffering comes from craving, and ending desire ends suffering",
      religion: "Buddhism",
      keywords: ["buddhism suffering desire", "craving causes suffering", "end of desire nirvana", "second noble truth"],
      sourceQuote: { ref: "Dhammapada, ch. 24 (Craving)", text: "From craving springs grief, from craving springs fear; for him who is wholly free from craving there is no grief, much less fear." }, // VERIFY WORDING/verse number
      note: "This reflects the Second and Third Noble Truths: craving (tanha) is the origin of suffering, and its cessation is the path to Nirvana.",
      verses: [
        { ref: "Matthew 11:28-29", text: "Come unto me, all ye that labour and are heavy laden, and I will give you rest. Take my yoke upon you, and learn of me; for I am meek and lowly in heart: and ye shall find rest unto your souls." },
        { ref: "John 10:10", text: "The thief cometh not, but for to steal, and to kill, and to destroy: I am come that they might have life, and that they might have it more abundantly." }
      ]
    },
    {
      claim: "There is no eternal, unchanging soul (anatta)",
      religion: "Buddhism",
      keywords: ["anatta", "no soul buddhism", "no self buddhist", "impermanence self"],
      positionSummary: "Buddhism teaches anatta (\"non-self\") — that what we call the self is a changing bundle of processes, with no permanent, unchanging soul underlying it.",
      verses: [
        { ref: "Ecclesiastes 3:11", text: "He hath made every thing beautiful in his time: also he hath set the world in their heart, so that no man can find out the work that God maketh from the beginning to the end." },
        { ref: "Psalm 42:1-2", text: "As the hart panteth after the water brooks, so panteth my soul after thee, O God. My soul thirsteth for God, for the living God: when shall I come and appear before God?" }
      ]
    },
    {
      claim: "Each person must strive for their own liberation; the Buddha is a teacher, not a savior",
      religion: "Buddhism",
      keywords: ["buddha only points the way", "no savior buddhism", "self-reliance liberation", "work out your own salvation buddhist"],
      sourceQuote: { ref: "Dhammapada 276", text: "You yourselves must do the work, the Realized Ones just show the way. Meditators practicing absorption are released from Māra's bonds." },
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "The highest goal is Nirvana, the extinguishing of the self and all craving — not everlasting conscious life with a personal God",
      religion: "Buddhism",
      keywords: ["nirvana extinguishment", "nibbana highest happiness", "buddhist final goal", "cessation not eternal life"],
      sourceQuote: { ref: "Dhammapada 204", text: "Health is the ultimate blessing; contentment, the ultimate wealth; trust is the ultimate family; extinguishment, the ultimate happiness." },
      note: "Nibbana (\"extinguishment\") — release from the cycle of craving and rebirth — is presented here as the highest happiness attainable.",
      verses: [
        { ref: "John 17:3", text: "And this is life eternal, that they might know thee the only true God, and Jesus Christ, whom thou hast sent." },
        { ref: "Revelation 21:3-4", text: "...Behold, the tabernacle of God is with men, and he will dwell with them, and they shall be his people, and God himself shall be with them, and be their God. And God shall wipe away all tears from their eyes; and there shall be no more death, neither sorrow, nor crying..." }
      ]
    },
    {
      claim: "There's no evidence for God",
      religion: "Atheism",
      keywords: ["no evidence for god", "atheism proof god", "no proof god exists"],
      positionSummary: "A common atheist position holds that belief in God lacks sufficient empirical evidence, and that natural explanations account for the universe without needing a creator.",
      verses: [
        { ref: "Romans 1:20", text: "For the invisible things of him from the creation of the world are clearly seen, being understood by the things that are made, even his eternal power and Godhead; so that they are without excuse:" },
        { ref: "Psalm 19:1", text: "The heavens declare the glory of God; and the firmament sheweth his handywork." }
      ]
    },
    {
      claim: "Morality doesn't require God",
      religion: "Atheism",
      keywords: ["morality without god", "atheist ethics", "can you be good without god"],
      positionSummary: "Many atheists argue that moral behavior arises from evolved social instincts, empathy, and reason, and does not depend on belief in a deity.",
      verses: [
        { ref: "Romans 2:14-15", text: "For when the Gentiles, which have not the law, do by nature the things contained in the law, these, having not the law, are a law unto themselves: Which shew the work of the law written in their hearts, their conscience also bearing witness, and their thoughts the mean while accusing or else excusing one another;" }
      ]
    },
    {
      claim: "The universe can be explained by natural causes alone, without need of a Creator",
      religion: "Atheism",
      keywords: ["universe without god", "big bang no creator", "abiogenesis atheism", "natural causes not god"],
      positionSummary: "Many atheists hold that cosmological models such as the Big Bang, and the possibility of abiogenesis, show the universe and life can in principle be accounted for by natural law and chance, without requiring a supernatural first cause.",
      verses: [
        { ref: "Genesis 1:1", text: "In the beginning God created the heaven and the earth." },
        { ref: "Hebrews 11:3", text: "Through faith we understand that the worlds were framed by the word of God, so that things which are seen were not made of things which do appear." },
        { ref: "Psalm 33:6", text: "By the word of the LORD were the heavens made; and all the host of them by the breath of his mouth." }
      ]
    },
    {
      claim: "The existence of evil and suffering is incompatible with an all-powerful, all-good God",
      religion: "Atheism",
      keywords: ["problem of evil", "suffering disproves god", "atheism argument from evil", "omnipotent good god suffering"],
      positionSummary: "The classical \"problem of evil\" argues that the amount and severity of suffering in the world is difficult to reconcile with a God who is simultaneously all-powerful and perfectly good — if he could prevent evil and doesn't, he isn't good; if he would but can't, he isn't all-powerful.",
      verses: [
        { ref: "Genesis 3:17-19", text: "...cursed is the ground for thy sake; in sorrow shalt thou eat of it all the days of thy life... In the sweat of thy face shalt thou eat bread, till thou return unto the ground..." },
        { ref: "Romans 8:20-22", text: "For the creature was made subject to vanity, not willingly, but by reason of him who hath subjected the same in hope... For we know that the whole creation groaneth and travaileth in pain together until now." },
        { ref: "Revelation 21:4", text: "And God shall wipe away all tears from their eyes; and there shall be no more death, neither sorrow, nor crying, neither shall there be any more pain: for the former things are passed away." }
      ]
    },
    {
      claim: "No single religion has exclusive access to truth",
      religion: "Pluralism",
      keywords: ["all religions equally true", "no exclusive truth", "religious pluralism"],
      positionSummary: "Religious pluralism holds that multiple, even conflicting, religious traditions can each contain genuine (if partial) truth and lead toward the same ultimate reality.",
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Sincerity matters more than which religion you follow",
      religion: "Pluralism",
      keywords: ["sincere belief enough", "sincerity over doctrine", "as long as you're sincere"],
      positionSummary: "A common pluralist view is that sincere devotion within any tradition is what matters, rather than the specific content of what is believed.",
      verses: [
        { ref: "Matthew 7:21-23", text: "Not every one that saith unto me, Lord, Lord, shall enter into the kingdom of heaven; but he that doeth the will of my Father which is in heaven. Many will say to me in that day, Lord, Lord, have we not prophesied in thy name? and in thy name have cast out devils? and in thy name done many wonderful works? And then will I profess unto them, I never knew you: depart from me, ye that work iniquity." }
      ]
    },
    {
      claim: "All major religions are different paths up the same mountain, teaching the same essential truths",
      religion: "Pluralism",
      keywords: ["paths up the same mountain", "perennialism", "all religions same essential truth", "transcendent unity of religions"],
      positionSummary: "Perennialist pluralism holds that beneath surface differences in ritual and doctrine, the major world religions converge on the same core spiritual and ethical truths, like different paths up a single mountain toward the same summit.",
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Isaiah 45:5", text: "I am the LORD, and there is none else, there is no God beside me..." }
      ]
    },
    {
      claim: "Insisting that only one religion is true is arrogant and the cause of religious conflict",
      religion: "Pluralism",
      keywords: ["exclusivism arrogant", "religious conflict caused by exclusivity", "intolerant to claim one true religion"],
      positionSummary: "A common pluralist objection holds that exclusive truth-claims are inherently intolerant, and that much religious conflict throughout history stems from this insistence rather than from any one religion's specific content.",
      verses: [
        { ref: "1 Peter 3:15", text: "But sanctify the Lord God in your hearts: and be ready always to give an answer to every man that asketh you a reason of the hope that is in you with meekness and fear:" },
        { ref: "Ephesians 4:15", text: "But speaking the truth in love, may grow up into him in all things, which is the head, even Christ:" }
      ]
    },
    {
      claim: "Humans determine their own values without appeal to the supernatural",
      religion: "Humanism",
      keywords: ["secular humanism values", "human-centered ethics", "no supernatural morality"],
      positionSummary: "Secular Humanism (cf. Humanist Manifesto III) holds that ethical values are derived from human experience and reason, without reference to a divine lawgiver.",
      verses: [
        { ref: "Jeremiah 10:23", text: "O LORD, I know that the way of man is not in himself: it is not in man that walketh to direct his steps." },
        { ref: "Proverbs 3:5-6", text: "Trust in the LORD with all thine heart; and lean not unto thine own understanding. In all thy ways acknowledge him, and he shall direct thy paths." }
      ]
    },
    {
      claim: "Reason and science are sufficient guides for life",
      religion: "Humanism",
      keywords: ["reason and science enough", "humanist rationalism", "science as guide for life"],
      positionSummary: "Humanists generally hold that reason, evidence, and scientific inquiry — not revelation — are the most reliable guides to truth and to living well.",
      verses: [
        { ref: "Proverbs 1:7", text: "The fear of the LORD is the beginning of knowledge: but fools despise wisdom and instruction." }
      ]
    },
    {
      claim: "Human dignity and rights derive from our shared humanity, not from being created in God's image",
      religion: "Humanism",
      keywords: ["human dignity secular", "rights from humanity not god", "humanist dignity without imago dei"],
      positionSummary: "Secular Humanism grounds human dignity and rights in our common humanity and capacity for reason and empathy, rather than in being made in the image of a Creator.",
      verses: [
        { ref: "Genesis 1:27", text: "So God created man in his own image, in the image of God created he him; male and female created he them." },
        { ref: "Psalm 8:4-5", text: "What is man, that thou art mindful of him? and the son of man, that thou visitest him? For thou hast made him a little lower than the angels, and hast crowned him with glory and honour." }
      ]
    },
    {
      claim: "This present life is the only one we have, so meaning must be found and created within it",
      religion: "Humanism",
      keywords: ["this life is all there is", "humanist manifesto no afterlife", "secular meaning in this life only"],
      positionSummary: "Humanist Manifestos describe this present life as \"all and enough,\" holding that since there is no evidence of an afterlife, human beings should find and create meaning, joy, and ethical purpose within their one earthly life.",
      verses: [
        { ref: "1 Corinthians 15:19", text: "If in this life only we have hope in Christ, we are of all men most miserable." },
        { ref: "2 Corinthians 4:18", text: "While we look not at the things which are seen, but at the things which are not seen: for the things which are seen are temporal; but the things which are not seen are eternal." }
      ]
    },
    {
      claim: "Lucifer represents enlightenment and self-liberation, not evil",
      religion: "Luciferianism",
      keywords: ["luciferianism lightbringer", "lucifer enlightenment", "lucifer not satan"],
      positionSummary: "Some Luciferian belief systems frame \"Lucifer\" (literally \"light-bringer\") as a symbol of illumination, knowledge, and self-liberation from imposed authority, rather than as a literal evil being. This varies significantly between individual adherents and groups — there is no single central text or authority.",
      verses: [
        { ref: "Isaiah 14:12-15", text: "How art thou fallen from heaven, O Lucifer, son of the morning! how art thou cut down to the ground, which didst weaken the nations! For thou hast said in thine heart, I will ascend into heaven, I will exalt my throne above the stars of God: I will sit also upon the mount of the congregation, in the sides of the north: I will ascend above the heights of the clouds; I will be like the most High. Yet thou shalt be brought down to hell, to the sides of the pit." },
        { ref: "2 Corinthians 11:14", text: "And no marvel; for Satan himself is transformed into an angel of light." }
      ]
    },
    {
      claim: "'Ye shall be as gods' is liberation, not deception",
      religion: "Luciferianism",
      keywords: ["ye shall be as gods", "as gods knowing good and evil", "genesis serpent liberation"],
      positionSummary: "Some Luciferian readings treat the serpent's offer in Genesis 3 as an invitation to enlightenment and self-determination, rather than as a lie that led to humanity's fall.",
      verses: [
        { ref: "Genesis 3:4-5", text: "And the serpent said unto the woman, Ye shall not surely die: For God doth know that in the day ye eat thereof, then your eyes shall be opened, and ye shall be as gods, knowing good and evil." },
        { ref: "Romans 6:23", text: "For the wages of sin is death; but the gift of God is eternal life through Jesus Christ our Lord." }
      ]
    },
    {
      claim: "God forbade the knowledge of good and evil out of jealousy, fearing humanity would become his equal",
      religion: "Luciferianism",
      keywords: ["god jealous of humanity", "become as one of us genesis", "luciferian reading of eden", "god fears competition"],
      sourceQuote: { ref: "Genesis 3:22", text: "And the LORD God said, Behold, the man is become as one of us, to know good and evil: and now, lest he put forth his hand, and take also of the tree of life, and eat, and live for ever:" },
      note: "Some Luciferian readings interpret God's exclusion of Adam and Eve from Eden not as a just response to disobedience, but as anxious self-protection by a threatened deity guarding forbidden knowledge and immortality for himself alone.",
      verses: [
        { ref: "James 1:13", text: "Let no man say when he is tempted, I am tempted of God: for God cannot be tempted with evil, neither tempteth he any man." },
        { ref: "Psalm 145:8-9", text: "The LORD is gracious, and full of compassion; slow to anger, and of great mercy. The LORD is good to all: and his tender mercies are over all his works." }
      ]
    },
    {
      claim: "Individual will and self-determination are sovereign; no external law or deity should bind the self",
      religion: "Luciferianism",
      keywords: ["sovereign self luciferian", "self as highest authority", "reject external divine law", "individual will supreme"],
      positionSummary: "A recurring Luciferian theme treats the sovereign, self-determining individual will as the highest authority, rejecting submission to any external divine law as a form of bondage to be thrown off.",
      verses: [
        { ref: "Proverbs 14:12", text: "There is a way which seemeth right unto a man, but the end thereof are the ways of death." },
        { ref: "James 4:7", text: "Submit yourselves therefore to God. Resist the devil, and he will flee from you." }
      ]
    },
    {
      claim: "All religions worship the same God under different names",
      religion: "Freemasonry",
      keywords: ["great architect of the universe", "freemasonry all religions same god", "masonic god"],
      positionSummary: "Masonic ritual refers to God as the \"Great Architect of the Universe,\" a deliberately generic title intended to be acceptable to members from different religious backgrounds, which in practice treats the specific identity of God as secondary to shared moral fraternity.",
      verses: [
        { ref: "Exodus 20:3", text: "Thou shalt have none other gods before me." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Freemasonry isn't a religion, just a moral fraternity",
      religion: "Freemasonry",
      keywords: ["freemasonry not a religion", "masonic fraternity morality", "is freemasonry a religion"],
      positionSummary: "Mainstream Masonic bodies officially describe Freemasonry as a fraternal, charitable organization built around shared moral teaching and symbolism, not a religion or a path to salvation — though it does require belief in a \"Supreme Being.\"",
      verses: [
        { ref: "James 1:22", text: "But be ye doers of the word, and not hearers only, deceiving your own selves." }
      ]
    },
    {
      claim: "The soul's immortality is taught through the allegory of Hiram Abiff being 'raised' from a symbolic death",
      religion: "Freemasonry",
      keywords: ["hiram abiff legend", "master mason raised allegory", "masonic third degree resurrection allegory"],
      positionSummary: "The Masonic third-degree (\"Master Mason\") ritual centers on the legend of Hiram Abiff, the Temple's builder, who is murdered and then symbolically \"raised\" — presented as an allegory teaching the immortality of the soul, distinct from any claim to an actual, historical resurrection.",
      verses: [
        { ref: "1 Corinthians 15:3-4", text: "For I delivered unto you first of all that which I also received, how that Christ died for our sins according to the scriptures; And that he was buried, and that he rose again the third day according to the scriptures:" },
        { ref: "John 11:25", text: "Jesus said unto her, I am the resurrection, and the life: he that believeth in me, though he were dead, yet shall he live:" }
      ]
    },
    {
      claim: "A morally upright life, summarized as being 'good and true,' is what makes a person acceptable to God",
      religion: "Freemasonry",
      keywords: ["masonic morality acceptable to god", "good and true mason", "celestial lodge above", "masonic works based acceptance"],
      positionSummary: "Masonic teaching emphasizes moral uprightness, brotherly love, and charitable works as what qualifies a soul for the \"Celestial Lodge above,\" without reference to salvation through Christ's atonement specifically.",
      verses: [
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." },
        { ref: "Romans 3:20", text: "Therefore by the deeds of the law there shall no flesh be justified in his sight: for by the law is the knowledge of sin." }
      ]
    },
    {
      claim: "Souls are purified in Purgatory after death before entering heaven",
      religion: "Catholicism",
      keywords: ["purgatory", "catholic purgatory doctrine", "temporal punishment after death", "purified after death"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 414", text: "Purgatory is the state in which those suffer for a time who die guilty of venial sins, or without having satisfied for the punishment due to their sins." },
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "Luke 23:43", text: "And Jesus said unto him, Verily I say unto thee, To day shalt thou be with me in paradise." },
        { ref: "2 Corinthians 5:8", text: "We are confident, I say, and willing rather to be absent from the body, and to be present with the Lord." }
      ]
    },
    {
      claim: "Mary was conceived without original sin (the Immaculate Conception)",
      religion: "Catholicism",
      keywords: ["immaculate conception", "mary sinless", "mary without original sin", "mary full of grace"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 50", text: "The Blessed Virgin Mary, through the merits of her divine Son, was preserved free from the guilt of Original Sin, and this privilege is called her Immaculate Conception." },
      note: "Defined as dogma by Pope Pius IX in the bull Ineffabilis Deus (1854).",
      verses: [
        { ref: "Romans 3:23", text: "For all have sinned, and come short of the glory of God." },
        { ref: "Romans 5:12", text: "Wherefore, as by one man sin entered into the world, and death by sin; and so death passed upon all men, for that all have sinned:" },
        { ref: "Luke 1:47", text: "And my spirit hath rejoiced in God my Saviour." }
      ]
    },
    {
      claim: "There is no salvation for anyone who knowingly remains outside the Catholic Church",
      religion: "Catholicism",
      keywords: ["no salvation outside the church", "extra ecclesiam nulla salus", "catholic church necessary for salvation"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 121", text: "All are bound to belong to the Church, and he who knows the Church to be the true Church and remains out of it, cannot be saved." },
      verses: [
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "Romans 10:9", text: "That if thou shalt confess with thy mouth the Lord Jesus, and shalt believe in thine heart that God hath raised him from the dead, thou shalt be saved." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." }
      ]
    },
    {
      claim: "The Pope, as successor of Peter, can teach infallibly on faith and morals",
      religion: "Catholicism",
      keywords: ["papal infallibility", "pope supreme authority", "petrine primacy", "vicar of christ"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 125", text: "The Church teaches infallibly when it speaks through the Pope and bishops united in general council, or through the Pope alone when he proclaims to all the faithful a doctrine of faith or morals." },
      note: "Papal infallibility exercised apart from a council was formally defined by the First Vatican Council in Pastor Aeternus (1870).",
      verses: [
        { ref: "Galatians 2:11", text: "But when Peter was come to Antioch, I withstood him to the face, because he was to be blamed." },
        { ref: "Acts 15:6-7", text: "And the apostles and elders came together for to consider of this matter. And when there had been much disputing, Peter rose up, and said unto them, Men and brethren, ye know how that a good while ago God made choice among us..." },
        { ref: "1 Peter 5:1", text: "The elders which are among you I exhort, who am also an elder, and a witness of the sufferings of Christ, and also a partaker of the glory that shall be revealed:" }
      ]
    },
    {
      claim: "Baptism is necessary for salvation",
      religion: "Catholicism",
      keywords: ["baptism necessary for salvation", "baptismal regeneration catholic", "baptism washes away sin"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 154", text: "Baptism is necessary to salvation, because without it we cannot enter into the kingdom of Heaven." },
      verses: [
        { ref: "Luke 23:43", text: "And Jesus said unto him, Verily I say unto thee, To day shalt thou be with me in paradise." },
        { ref: "Romans 10:9", text: "That if thou shalt confess with thy mouth the Lord Jesus, and shalt believe in thine heart that God hath raised him from the dead, thou shalt be saved." },
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." }
      ]
    },
    {
      claim: "The bread and wine of the Eucharist become the actual body and blood of Christ",
      religion: "Catholicism",
      keywords: ["transubstantiation", "real presence eucharist", "catholic communion body of christ", "eucharist literal body and blood"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 244, 246", text: "After the substance of the bread and wine had been changed into the substance of the body and blood of Our Lord there remained only the appearances of bread and wine. This change of the bread and wine into the body and blood of Our Lord is called Transubstantiation." },
      verses: [
        { ref: "Luke 22:19", text: "And he took bread, and gave thanks, and brake it, and gave unto them, saying, This is my body which is given for you: this do in remembrance of me." },
        { ref: "John 6:63", text: "It is the spirit that quickeneth; the flesh profiteth nothing: the words that I speak unto you, they are spirit, and they are life." }
      ]
    },
    {
      claim: "Mortal sins must be confessed to a priest to be absolved",
      religion: "Catholicism",
      keywords: ["confession to a priest", "catholic confession sin", "sacrament of penance", "auricular confession"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 209", text: "We are bound to confess all our mortal sins, but it is well also to confess our venial sins." },
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "1 John 1:9", text: "If we confess our sins, he is faithful and just to forgive us our sins, and to cleanse us from all unrighteousness." }
      ]
    },
    {
      claim: "The saints can be invoked in prayer to ask for their help and intercession",
      religion: "Catholicism",
      keywords: ["intercession of saints", "praying to saints", "mary mediatrix", "invocation of saints catholic"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 332, 333", text: "The First Commandment does not forbid us to pray to the saints. By praying to the saints we mean the asking of their help and prayers." },
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "Hebrews 4:16", text: "Let us therefore come boldly unto the throne of grace, that we may obtain mercy, and find grace to help in time of need." }
      ]
    },
    {
      claim: "Grace is necessary to merit Heaven — good works done in grace merit salvation",
      religion: "Catholicism",
      keywords: ["merit catholic salvation", "faith and works catholic", "cooperate with grace", "earn salvation catholic"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 111", text: "Grace is necessary for salvation, because without grace we can do nothing to merit Heaven." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "The Church can grant indulgences that remit the temporal punishment due for sin",
      religion: "Catholicism",
      keywords: ["indulgences catholic", "remission of temporal punishment", "catholic indulgence doctrine"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 231", text: "An indulgence is the remission in whole or in part of the temporal punishment due to sin." },
      verses: [
        { ref: "Ephesians 1:7", text: "In whom we have redemption through his blood, the forgiveness of sins, according to the riches of his grace;" },
        { ref: "Colossians 2:13-14", text: "Having forgiven you all trespasses; Blotting out the handwriting of ordinances that was against us, which was contrary to us, and took it out of the way, nailing it to his cross;" }
      ]
    },
    {
      claim: "Justification by faith alone, with nothing else required, is formally condemned as anathema",
      religion: "Catholicism",
      keywords: ["sola fide anathema", "faith alone condemned catholic", "council of trent justification", "trent canon on faith alone"],
      sourceQuote: { ref: "Council of Trent, Session the Sixth (On Justification), Canon IX", text: "If any one saith, that by faith alone the impious is justified; in such wise as to mean, that nothing else is required to co-operate in order to the obtaining the grace of Justification, and that it is not in any way necessary, that he be prepared and disposed by the movement of his own will; let him be anathema." },
      note: "This is Trent's direct, formal condemnation of sola fide, the doctrine that justification is by faith alone.",
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Romans 3:28", text: "Therefore we conclude that a man is justified by faith without the deeds of the law." },
        { ref: "Galatians 2:16", text: "Knowing that a man is not justified by the works of the law, but by the faith of Jesus Christ, even we have believed in Jesus Christ, that we might be justified by the faith of Christ, and not by the works of the law: for by the works of the law shall no flesh be justified." }
      ]
    },
    /*
     * These two quote the Second Vatican Council directly (Lumen Gentium,
     * 1964; Nostra Aetate, 1965), not the Baltimore Catechism above. Unlike
     * that text, no confirmed-public-domain English translation of either
     * document exists — every edition in circulation is claimed under
     * copyright by some party (Libreria Editrice Vaticana, or Sheed & Ward /
     * Trustees for Roman Catholic Purposes for the Flannery edition). So
     * these are kept to a single short, attributed excerpt each rather than
     * a bulk corpus like the other sources in this file.
     */
    {
      claim: "Those who through no fault of their own never come to know Christ or the Church can still attain salvation",
      religion: "Catholicism",
      keywords: ["invincible ignorance salvation", "lumen gentium salvation", "saved without knowing christ catholic", "sincere seekers of god saved"],
      sourceQuote: { ref: "Lumen Gentium §16 (Second Vatican Council, 1964)", text: "Those also can attain to everlasting salvation who through no fault of their own do not know the Gospel of Christ or His Church, yet sincerely seek God and, moved by grace, try in their actions to do His will as they know it through the dictates of their conscience." }, // VERIFY WORDING — translation edition
      verses: [
        { ref: "Romans 10:14", text: "How then shall they call on him in whom they have not believed? and how shall they believe in him of whom they have not heard? and how shall they hear without a preacher?" },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." }
      ]
    },
    {
      claim: "Non-Christian religions can reflect a genuine ray of the truth that enlightens all people",
      religion: "Catholicism",
      keywords: ["nostra aetate other religions", "rays of truth other religions catholic", "catholic view of world religions", "vatican ii non-christian religions"],
      sourceQuote: { ref: "Nostra Aetate §2 (Second Vatican Council, 1965)", text: "The Catholic Church rejects nothing that is true and holy in these religions... these often reflect a ray of that Truth which enlightens all men." }, // VERIFY WORDING — translation edition
      verses: [
        { ref: "John 14:6", text: "Jesus saith unto him, I am the way, the truth, and the life: no man cometh unto the Father, but by me." },
        { ref: "Acts 4:12", text: "Neither is there salvation in any other: for there is none other name under heaven given among men, whereby we must be saved." },
        { ref: "2 Corinthians 6:14", text: "Be ye not unequally yoked together with unbelievers: for what fellowship hath righteousness with unrighteousness? and what communion hath light with darkness?" }
      ]
    },
    /*
     * These three fill gaps spotted by cross-checking against lotwi.org's
     * "Catholicism" objections page (Light of the World Initiative) — used
     * only to find topics not yet covered here, not as a text source: its
     * own written responses are that ministry's original copyrighted
     * commentary, not reused below. Each entry here is independently
     * sourced against this app's own verified Baltimore Catechism data,
     * or (where no pre-1929 public-domain primary source exists for a
     * dogma) written as a plain positionSummary instead of inventing a
     * quote.
     */
    {
      claim: "The Pope is Peter's successor and, as visible head of the Church, holds supreme authority over it",
      religion: "Catholicism",
      keywords: ["pope successor of peter", "petrine succession", "vicar of christ visible head", "papal supreme authority"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 117, 118", text: "Our Holy Father the Pope, the Bishop of Rome, is the vicar of Christ on earth and the visible head of the Church... because he is the successor of St. Peter, whom Christ made the chief of the Apostles and the visible head of the Church." },
      note: "Distinct from papal infallibility (a specific teaching mechanism) — this is the broader claim of Peter's succession and headship itself.",
      verses: [
        { ref: "Colossians 1:18", text: "And he is the head of the body, the church: who is the beginning, the firstborn from the dead; that in all things he might have the preeminence." },
        { ref: "Matthew 23:9", text: "And call no man your father upon the earth: for one is your Father, which is in heaven." },
        { ref: "Ephesians 1:22", text: "And hath put all things under his feet, and gave him to be the head over all things to the church," }
      ]
    },
    {
      claim: "Mary was bodily assumed into heaven at the end of her earthly life",
      religion: "Catholicism",
      keywords: ["assumption of mary", "mary taken up to heaven", "bodily assumption catholic"],
      positionSummary: "Defined as dogma by Pope Pius XII in Munificentissimus Deus (1950): that Mary, having completed her earthly life, was assumed body and soul into heavenly glory. No pre-1929 public-domain primary source exists for this one — it was formally defined after that cutoff — so it's stated here rather than quoted.",
      verses: [
        { ref: "John 3:13", text: "And no man hath ascended up to heaven, but he that came down from heaven, even the Son of man which is in heaven." },
        { ref: "Acts 13:36", text: "For David, after he had served his own generation by the will of God, fell on sleep, and was laid unto his fathers, and saw corruption:" }
      ]
    },
    {
      claim: "Priests must remain celibate and unmarried to serve God more fully",
      religion: "Catholicism",
      keywords: ["priestly celibacy", "catholic priests cannot marry", "clerical celibacy discipline"],
      positionSummary: "Latin Rite discipline (not dogma) requires priests to remain unmarried, understood as freeing them for undivided devotion to ministry.",
      verses: [
        { ref: "1 Timothy 4:1-3", text: "Now the Spirit speaketh expressly, that in the latter times some shall depart from the faith, giving heed to seducing spirits, and doctrines of devils; Speaking lies in hypocrisy; having their conscience seared with a hot iron; Forbidding to marry, and commanding to abstain from meats, which God hath created to be received with thanksgiving of them which believe and know the truth." },
        { ref: "1 Corinthians 9:5", text: "Have we not power to lead about a sister, a wife, as well as other apostles, and as the brethren of the Lord, and Cephas?" },
        { ref: "1 Timothy 3:2", text: "A bishop then must be blameless, the husband of one wife, vigilant, sober, of good behaviour, given to hospitality, apt to teach;" }
      ]
    },
    {
      claim: "Sacred images and statues may be venerated because they are reminders of Christ and the saints, not gods themselves",
      religion: "Catholicism",
      keywords: ["veneration of images catholic", "statues not idols", "catholic images second commandment", "praying before images"],
      sourceQuote: { ref: "Baltimore Catechism, Q. 341", text: "The First Commandment does forbid the making of images if they are made to be adored as gods, but it does not forbid the making of them to put us in mind of Jesus Christ, His Blessed Mother, and the saints." },
      note: "Q. 343 of the same catechism is explicit that Catholics are taught not to pray to the images themselves: \"It is not allowed to pray to the crucifix or images and relics of the saints, for they have no life, nor power to help us, nor sense to hear us.\"",
      verses: [
        { ref: "Exodus 20:4-5", text: "Thou shalt not make unto thee any graven image, or any likeness of any thing that is in heaven above, or that is in the earth beneath, or that is in the water under the earth: Thou shalt not bow down thyself to them, nor serve them..." },
        { ref: "Isaiah 44:9", text: "They that make a graven image are all of them vanity; and their delectable things shall not profit..." }
      ]
    },
    {
      claim: "The Mass is a true, propitiatory sacrifice offered for the living and the dead, not merely a commemoration of Calvary",
      religion: "Catholicism",
      keywords: ["sacrifice of the mass", "mass propitiatory sacrifice", "trent mass canon", "eucharist re-presents calvary"],
      sourceQuote: { ref: "Council of Trent, Session the Twenty-Second (On The Sacrifice Of The Mass), Canon III", text: "If any one saith, that the sacrifice of the mass is only a sacrifice of praise and of thanksgiving; or, that it is a bare commemoration of the sacrifice consummated on the cross, but not a propitiatory sacrifice; or, that it profits him only who receives; and that it ought not to be offered for the living and the dead for sins, pains, satisfactions, and other necessities; let him be anathema." },
      verses: [
        { ref: "Hebrews 9:25-28", text: "Nor yet that he should offer himself often... but now once in the end of the world hath he appeared to put away sin by the sacrifice of himself... so Christ was once offered to bear the sins of many..." },
        { ref: "Hebrews 10:12-14", text: "But this man, after he had offered one sacrifice for sins for ever, sat down on the right hand of God... For by one offering he hath perfected for ever them that are sanctified." }
      ]
    },
    /*
     * These three are tagged "Apocrypha", not "Catholicism" — they're
     * specific claims found within the deuterocanonical books themselves
     * (see the searchable Apocrypha/Deuterocanon source text above), which
     * is historically why the Reformers rejected these books' canonicity:
     * not on manuscript grounds alone, but because passages like these
     * conflict with sola fide/sola gratia. Framed as claims from the text
     * itself, not as claims about what Catholics believe.
     */
    {
      claim: "Prayer for the dead is holy and wholesome, and can free them from their sins",
      religion: "Apocrypha",
      keywords: ["prayer for the dead", "pray for the dead purgatory", "2 maccabees prayer for dead", "loosed from sins after death"],
      sourceQuote: { ref: "2 Machabees 12:46", text: "It is therefore a holy and wholesome thought to pray for the dead, that they may be loosed from sins." },
      note: "This passage, describing Judas Machabeus taking up a collection for a sin offering on behalf of fallen soldiers, is the primary text cited in support of prayer for the dead and Purgatory.",
      verses: [
        { ref: "Hebrews 9:27", text: "And as it is appointed unto men once to die, but after this the judgment:" },
        { ref: "Luke 16:26", text: "And beside all this, between us and you there is a great gulf fixed: so that they which would pass from hence to you cannot; neither can they pass to us, that would come from thence." },
        { ref: "2 Corinthians 5:10", text: "For we must all appear before the judgment seat of Christ; that every one may receive the things done in his body, according to that he hath done, whether it be good or bad." }
      ]
    },
    {
      claim: "Almsgiving delivers from death and purges away sin",
      religion: "Apocrypha",
      keywords: ["alms purge sin", "almsgiving delivers from death", "tobit alms", "works atone for sin"],
      sourceQuote: { ref: "Tobias 12:9", text: "For alms delivereth from death, and the same is that which purgeth away sins, and maketh to find mercy and life everlasting." },
      verses: [
        { ref: "Ephesians 2:8-9", text: "For by grace are ye saved through faith; and that not of yourselves: it is the gift of God: Not of works, lest any man should boast." },
        { ref: "Isaiah 64:6", text: "But we are all as an unclean thing, and all our righteousnesses are as filthy rags; and we all do fade as a leaf; and our iniquities, like the wind, have taken us away." },
        { ref: "Titus 3:5", text: "Not by works of righteousness which we have done, but according to his mercy he saved us, by the washing of regeneration, and renewing of the Holy Ghost;" }
      ]
    },
    {
      claim: "God left man entirely in the hand of his own free counsel to choose good or evil",
      religion: "Apocrypha",
      keywords: ["free will apocrypha", "sirach free will", "hand of his own counsel", "stretch forth thy hand to which thou wilt"],
      sourceQuote: { ref: "Ecclesiasticus 15:14", text: "God made man from the beginning, and left him in the hand of his own counsel." },
      note: "The passage continues in 15:17: \"He hath set water and fire before thee: stretch forth thy hand to which thou wilt.\"",
      verses: [
        { ref: "Ephesians 2:1", text: "And you hath he quickened, who were dead in trespasses and sins;" },
        { ref: "John 15:16", text: "Ye have not chosen me, but I have chosen you, and ordained you, that ye should go and bring forth fruit, and that your fruit should remain: that whatsoever ye shall ask of the Father in my name, he may give it you." },
        { ref: "Romans 9:16", text: "So then it is not of him that willeth, nor of him that runneth, but of God that sheweth mercy." }
      ]
    },
    {
      claim: "The deuterocanonical books belong in the Bible as inspired Scripture",
      religion: "Apocrypha",
      keywords: ["apocrypha canonicity", "deuterocanon inspired scripture", "apocrypha belongs in the bible", "canon of scripture apocrypha"],
      positionSummary: "The Council of Trent (1546) formally defined these books as canonical Scripture, against the Reformers, who excluded them, following the Jewish canon — the Hebrew Bible never included them, and the New Testament, while it quotes the Old Testament heavily, never quotes any of these books as Scripture.",
      verses: [
        { ref: "Luke 24:44", text: "And he said unto them, These are the words which I spake unto you, while I was yet with you, that all things must be fulfilled, which were written in the law of Moses, and in the prophets, and in the psalms, concerning me." },
        { ref: "Romans 3:2", text: "Much every way: chiefly, because that unto them were committed the oracles of God." }
      ]
    },
    {
      claim: "Departed righteous souls in heaven pray for and intercede on behalf of the living",
      religion: "Apocrypha",
      keywords: ["intercession of the dead apocrypha", "onias jeremiah vision", "2 maccabees departed saints pray", "saints in heaven intercede"],
      sourceQuote: { ref: "2 Machabees 15:12-14", text: "Onias, who had been high priest, a good and virtuous man... prayed for all the people of the Jews... This is a lover of his brethren, and of the people of Israel: this is he that prayeth much for the people, and for all the holy city, Jeremias, the prophet of God." },
      note: "Judas Machabeus's vision of the deceased Onias and the prophet Jeremiah interceding for Israel from beyond death is cited in support of the intercession of departed saints.",
      verses: [
        { ref: "1 Timothy 2:5", text: "For there is one God, and one mediator between God and men, the man Christ Jesus;" },
        { ref: "Ecclesiastes 9:5", text: "For the living know that they shall die: but the dead know not any thing, neither have they any more a reward; for the memory of them is forgotten." }
      ]
    },
    {
      claim: "The relics of holy men retain miraculous power even after death",
      religion: "Apocrypha",
      keywords: ["relics power apocrypha", "elisha bones prophesied", "ecclesiasticus relics", "sirach body prophesied after death"],
      sourceQuote: { ref: "Ecclesiasticus 48:14", text: "No word could overcome him, and after death his body prophesied." },
      note: "Said of the prophet Eliseus (Elisha); compare 2 Kings 13:20-21, where a dead man is restored to life on touching Elisha's bones.",
      verses: [
        { ref: "Deuteronomy 34:5-6", text: "So Moses the servant of the LORD died there in the land of Moab... and he buried him... but no man knoweth of his sepulchre unto this day." },
        { ref: "Acts 14:14-15", text: "Which when the apostles, Barnabas and Paul, heard of, they rent their clothes, and ran in among the people, crying out, And saying, Sirs, why do ye these things? We also are men of like passions with you..." }
      ]
    }
  ];

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  // BibleHub-style keyword highlighting: bold/mark every whole-word occurrence
  // of any searched term inside the (already-escaped) verse text.
  function highlightText(safeText, query) {
    const terms = [...new Set(
      query.trim().split(/\s+/).filter(Boolean).map(t => t.toLowerCase())
    )].filter(t => t.length > 1).sort((a, b) => b.length - a.length);

    if (terms.length === 0) return safeText;

    const pattern = terms.map(escapeRegex).join('|');
    const re = new RegExp(`\\b(${pattern})`, 'gi');
    return safeText.replace(re, '<mark class="hl">$1</mark>');
  }

  function renderTopicOverview(query, allMatchedEntries, bibleMatches) {
    const container = document.getElementById('topic-overview');
    const trimmed = query.trim();

    if (trimmed.length < 3) { container.style.display = 'none'; container.innerHTML = ''; return; }

    const religions = [...new Set(allMatchedEntries.map(e => e.religion))];
    const verseMatches = bibleMatches.slice(0, 3).map(m => m.item);

    if (religions.length === 0 && verseMatches.length === 0) {
      container.style.display = 'none';
      container.innerHTML = '';
      return;
    }

    const religionsHtml = religions.length
      ? `<p class="topic-religions">Traditions touching this: ${religions.map(escapeHtml).join(', ')}</p>`
      : `<p class="topic-religions">No tagged tradition entries matched this — showing Scripture only.</p>`;

    const versesHtml = verseMatches.length
      ? `<div class="topic-verses">${verseMatches.map(v => `
          <div class="verse-block">
            <a class="stamp" href="${bibleLink(v.ref)}" target="_blank" rel="noopener noreferrer">${v.ref} ↗</a>
            <p>"${highlightText(escapeHtml(v.text), trimmed)}"</p>
          </div>
        `).join('')}</div>`
      : (bibleIndexReady
          ? ''
          : `<p class="form-hint">Connect once to download the full Bible so Scripture matches show here too.</p>`);

    container.innerHTML = `
      <div class="topic-label">Search</div>
      <h2 class="topic-title">"${escapeHtml(trimmed)}"</h2>
      ${religionsHtml}
      ${versesHtml}
    `;
    container.style.display = 'block';
  }

  const searchInput = document.getElementById('search');
  const chips = document.querySelectorAll('.side-chip');
  const resultsEl = document.getElementById('results');
  const metaEl = document.getElementById('results-meta');
  const emptyEl = document.getElementById('empty');
  const textFilterSelect = document.getElementById('text-filter');
  const paginationEl = document.getElementById('results-pagination');

  let activeFilter = 'all';
  let activeSourceId = null; // narrows activeFilter down to one specific SOURCE_TEXTS entry, via the text-filter dropdown

  // Argument-card pagination: finalizeRender computes the full, sorted match
  // list once per search and caches it here; clicking a dot just re-slices
  // and re-renders from the cache instead of re-running the search.
  const RESULTS_PAGE_SIZE = 7;
  let currentMatches = [];
  let currentResultsPage = 0;

  function renderResultsPagination() {
    if (!paginationEl) return;
    const totalPages = Math.ceil(currentMatches.length / RESULTS_PAGE_SIZE);
    if (totalPages <= 1) {
      paginationEl.style.display = 'none';
      paginationEl.innerHTML = '';
      return;
    }
    paginationEl.style.display = 'flex';
    paginationEl.innerHTML = Array.from({ length: totalPages }, (_, i) =>
      `<button class="page-dot${i === currentResultsPage ? ' active' : ''}" data-page="${i}" aria-label="Page ${i + 1} of ${totalPages}"></button>`
    ).join('');
    paginationEl.querySelectorAll('[data-page]').forEach(dot => {
      dot.addEventListener('click', () => {
        currentResultsPage = parseInt(dot.dataset.page, 10);
        renderResultsPage();
      });
    });
  }

  function renderResultsPage() {
    const start = currentResultsPage * RESULTS_PAGE_SIZE;
    const pageItems = currentMatches.slice(start, start + RESULTS_PAGE_SIZE);
    resultsEl.innerHTML = '';
    pageItems.forEach(entry => resultsEl.appendChild(buildEntryCard(entry)));
    renderResultsPagination();
  }

  function bibleLink(ref) {
    const clean = ref.replace(/\s*\(.*?\)\s*$/, '').trim();
    return `https://www.biblegateway.com/passage/?search=${encodeURIComponent(clean)}&version=KJV`;
  }

  function quranLink(ref) {
    const match = ref.match(/(\d+)\s*:\s*(\d+)/);
    if (!match) return null;
    return `https://quran.com/${match[1]}/${match[2]}`;
  }

  function sourceLink(entry) {
    if (!entry.sourceQuote) return null;
    if (entry.sourceQuote.link) return entry.sourceQuote.link;
    if (/qur'?an/i.test(entry.sourceQuote.ref)) return quranLink(entry.sourceQuote.ref);
    if (/\d+:\d+/.test(entry.sourceQuote.ref) && /^(Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua|Judges|Ruth|Samuel|Kings|Chronicles|Ezra|Nehemiah|Esther|Job|Psalm|Proverbs|Ecclesiastes|Song|Isaiah|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans|Corinthians|Galatians|Ephesians|Philippians|Colossians|Thessalonians|Timothy|Titus|Philemon|Hebrews|James|Peter|Jude|Revelation)/.test(entry.sourceQuote.ref)) {
      return bibleLink(entry.sourceQuote.ref);
    }
    return null;
  }

  const STOPWORDS = new Set(["the","and","are","was","were","that","this","with","from","have","has","had",
    "for","you","your","they","them","into","than","then","who","what","when","where","why","how","does",
    "did","can","could","would","should","just","only","also","very","more","most","which","there","here",
    "about","its","it's","isn't","isnt","doesn't","doesnt","dont","don't","not","did","really"]);

  function normalizeText(str) {
    return str.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  }

  function tokenize(str) {
    return normalizeText(str).split(' ').filter(w => w.length > 2 && !STOPWORDS.has(w));
  }

  /* ===== Typo-tolerant word correction ===== */

  const EXTRA_VOCAB = [
    "resurrection","crucifixion","salvation","reincarnation","trinity","atonement","forgiveness",
    "repentance","baptism","prophecy","scripture","covenant","commandments","sabbath","genesis",
    "creation","judgment","righteousness","sanctification","justification","redemption","grace",
    "gospel","apostle","disciple","messiah","prophet","idolatry","blasphemy","heresy","doctrine",
    "eternity","damnation","tribulation","rapture","millennium","antichrist","satan","demon",
    "angel","archangel","paradise","purgatory","reconciliation","intercession","omniscience",
    "omnipotence","omnipresence","incarnation","transfiguration","ascension","pentecost",
    "circumcision","sacrifice","tabernacle","synagogue","pharisee","sadducee","gentile","israelite",
    "patriarch","apostasy","predestination","providence","theodicy","eschatology","soteriology",
    "christology","pneumatology","hermeneutics","exegesis","monotheism","polytheism","pantheism",
    "agnosticism","secularism","materialism","nihilism","existentialism","enlightenment",
    "nirvana","karma","samsara","dharma","moksha","brahman","atman","tao","zen","sutra"
  ];

  const VOCAB = (() => {
    const words = new Set();
    ENTRIES.forEach(e => {
      tokenize(e.claim).forEach(w => words.add(w));
      e.keywords.forEach(k => tokenize(k).forEach(w => words.add(w)));
      if (e.positionSummary) tokenize(e.positionSummary).forEach(w => words.add(w));
    });
    EXTRA_VOCAB.forEach(w => words.add(w));
    return [...words];
  })();

  // Populated once the full Bible loads — gives us a large real-English dictionary
  // so ordinary words ("fire", "water", "mercy") are never "corrected" into something else.
  let realWordSet = new Set(VOCAB);

  function buildRealWordSet(verses) {
    const s = new Set(VOCAB);
    verses.forEach(v => {
      normalizeText(v.text).split(' ').forEach(w => { if (w.length > 2) s.add(w); });
    });
    realWordSet = s;
  }

  function levenshtein(a, b) {
    const m = a.length, n = b.length;
    if (m === 0) return n;
    if (n === 0) return m;
    const prev = new Array(n + 1);
    const curr = new Array(n + 1);
    for (let j = 0; j <= n; j++) prev[j] = j;
    for (let i = 1; i <= m; i++) {
      curr[0] = i;
      for (let j = 1; j <= n; j++) {
        const cost = a[i - 1] === b[j - 1] ? 0 : 1;
        curr[j] = Math.min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost);
      }
      for (let j = 0; j <= n; j++) prev[j] = curr[j];
    }
    return prev[n];
  }

  function correctWord(word) {
    if (word.length < 4) return word;
    // If it's a real word (in Scripture or our vocab), never touch it.
    if (realWordSet.has(word)) return word;
    // Conservative: only fix small typos, and only toward app-relevant vocabulary.
    const maxDist = word.length <= 6 ? 1 : 2;
    let best = null;
    let bestDist = maxDist + 1;
    for (const v of VOCAB) {
      if (Math.abs(v.length - word.length) > maxDist) continue;
      const d = levenshtein(word, v);
      if (d < bestDist) { bestDist = d; best = v; }
    }
    return (best && bestDist <= maxDist) ? best : word;
  }

  function correctQuery(query) {
    const words = query.trim().split(/\s+/).filter(Boolean);
    let changed = false;
    const corrected = words.map(w => {
      const lower = w.toLowerCase();
      const fixed = correctWord(lower);
      if (fixed !== lower) changed = true;
      return fixed;
    });
    return { corrected: corrected.join(' '), changed, original: query };
  }

  function score(entry, query) {
    if (!query) return 1;
    const qNorm = normalizeText(query);
    const qTokens = tokenize(query);
    if (qTokens.length === 0) return 1;

    const extraText = entry.sourceQuote ? entry.sourceQuote.text : (entry.positionSummary || '');
    const fieldsNorm = normalizeText([entry.claim, entry.religion, extraText].join(' '));

    let total = 0;

    entry.keywords.forEach(k => {
      const kNorm = normalizeText(k);
      if (kNorm.length < 4) return;
      if (qNorm === kNorm) { total += 20; return; }
      if (qNorm.includes(kNorm) || kNorm.includes(qNorm)) { total += 10; return; }
      const kTokens = tokenize(k);
      if (kTokens.length > 1) {
        const overlap = kTokens.filter(t => qTokens.includes(t)).length;
        if (overlap === kTokens.length) total += 8;
        else if (overlap > 0) total += overlap * 2;
      }
    });

    if (qNorm.length > 3 && fieldsNorm.includes(qNorm)) total += 15;
    const claimTokens = tokenize(entry.claim);
    const claimOverlap = qTokens.filter(t => claimTokens.includes(t)).length;
    total += claimOverlap * 3;

    const fieldTokenOverlap = qTokens.filter(t => fieldsNorm.includes(t)).length;
    total += fieldTokenOverlap;

    return total;
  }

  function buildEntryCard(entry) {
    const card = document.createElement('div');
    card.className = 'case';
    const srcLink = sourceLink(entry);
    const srcStamp = srcLink
      ? `<a class="stamp" href="${srcLink}" target="_blank" rel="noopener noreferrer">${entry.sourceQuote.ref} ↗</a>`
      : `<span class="stamp">${entry.sourceQuote ? entry.sourceQuote.ref : ''}</span>`;
    const claimContent = entry.sourceQuote
      ? `<div class="verse-block">
           ${srcStamp}
           <p>"${entry.sourceQuote.text}"</p>
         </div>${entry.note ? `<p class="teaching" style="margin-top:10px;font-size:13px;"><em>Context:</em> ${entry.note}</p>` : ''}`
      : `<p class="teaching"><em>No single canonical line to quote here.</em><br>${entry.positionSummary}</p>`;
    const eid = entryId(entry);
    const isBm = bookmarkIds.has(eid);
    card.innerHTML = `
      <div class="case-head">
        <h2>${entry.claim}</h2>
        <div class="card-actions">
          <span class="religion-tag">${entry.religion}</span>
          <button class="icon-btn share-btn" data-eid="${eid}" title="Share" aria-label="Share">⇪</button>
          <button class="icon-btn bookmark-btn ${isBm ? 'bookmarked' : ''}" data-eid="${eid}" title="Bookmark" aria-label="Bookmark">${isBm ? '★' : '☆'}</button>
        </div>
      </div>
      <div class="case-body">
        <div class="panel claim-panel">
          <div class="panel-label">Their own words</div>
          ${claimContent}
        </div>
        <div class="panel answer-panel">
          <div class="panel-label">Scripture's answer</div>
          ${entry.verses.map(v => `
            <div class="verse-block">
              <a class="stamp" href="${bibleLink(v.ref)}" target="_blank" rel="noopener noreferrer">${v.ref} ↗</a>
              <p>"${v.text}"</p>
            </div>
          `).join('')}
        </div>
      </div>
    `;
    card.querySelector('.bookmark-btn').addEventListener('click', () => toggleBookmark(entry));
    card.querySelector('.share-btn').addEventListener('click', () => shareEntry(entry));
    return card;
  }

  function entriesFor(q) {
    return q ? ENTRIES.filter(e => score(e, q) > 0) : [];
  }

  function currentSourceIdWhitelist() {
    if (activeSourceId) return sourceStatus[activeSourceId] === 'ready' ? [activeSourceId] : [];
    if (activeFilter === 'all') return null;
    return SOURCE_TEXTS.filter(s => s.tradition === activeFilter && sourceStatus[s.id] === 'ready').map(s => s.id);
  }

  // The worker processes messages strictly one at a time, in the order it
  // receives them, with no concept of "stale" — it will happily compute a
  // search for every keystroke of a fast typist even though only the last
  // one ever gets rendered. Left unchecked, that backlog is exactly what
  // turned a ~1.5s search into a 10+ second wait: 15 queued searches ahead
  // of the one you actually typed, each one blocking the next.
  //
  // The fix is a hard cap of one search in flight at a time. A new request
  // that arrives while one is still running doesn't queue behind it — it
  // just overwrites whatever was queued, so only the latest ever waits, and
  // it fires the instant the current search returns.
  let searchInFlight = false;
  let queuedSearch = null; // { rawQuery, query, phase } — most recent request received while one was in flight
  let searchDispatchedAt = 0;

  // Accumulates one reqId's replies from every pool worker before the search
  // is considered done — see handlePoolMessage. Only one in flight at a time
  // (searchInFlight/queuedSearch above), so a single pending record suffices.
  let poolPending = null;

  function dispatchSearch(rawQuery, query, phase) {
    const reqId = ++searchReqSeq;
    latestSearchReqId = reqId;
    searchInFlight = true;
    searchDispatchedAt = performance.now();
    poolPending = {
      reqId, rawQuery, query, phase,
      remaining: searchWorkers.length,
      bible: EMPTY_MATCHES,
      sourceTotal: 0,
      sourceParts: [],
      bibleMs: 0,
      maxSourceMs: 0,
      perSourceMs: {}
    };
    const sourceIdWhitelist = currentSourceIdWhitelist();
    searchWorkers.forEach(w => w.postMessage({ type: 'search', reqId, phase, rawQuery, query, sourceIdWhitelist }));
  }

  function requestSearch(rawQuery, query, phase) {
    if (searchInFlight) {
      queuedSearch = { rawQuery, query, phase };
      latestSearchReqId = ++searchReqSeq; // the in-flight response, once it arrives, is now stale
      return;
    }
    dispatchSearch(rawQuery, query, phase);
  }

  function render() {
    const rawQuery = searchInput.value.trim();

    // Below ~2 characters a fuzzy search across tens of thousands of rows is
    // mostly noise anyway — skip dispatching it and save the wasted work.
    if (!rawQuery || rawQuery.length < 2 || !searchWorkers.length) {
      queuedSearch = null;
      latestSearchReqId = ++searchReqSeq; // invalidate any in-flight/queued response from a previous call
      finalizeRender(rawQuery, rawQuery, false, entriesFor(rawQuery), EMPTY_MATCHES, EMPTY_MATCHES);
      return;
    }

    requestSearch(rawQuery, rawQuery, 'original');
  }

  // Each pool worker replies once per reqId with its own slice of results
  // (bible only ever comes from worker 0 — see loadBibleIntoWorkers). This
  // accumulates every worker's reply for the current reqId and only moves
  // on once all of them are in, same "one search in flight" guarantee the
  // single-worker version had, just fanned out across the pool.
  function handlePoolMessage(workerIndex, e) {
    const msg = e.data;
    if (msg.type !== 'search-result') return;
    if (!poolPending || msg.reqId !== poolPending.reqId) return; // stale — a newer request has since superseded this

    if (msg.bible.total > 0 || workerIndex === 0) poolPending.bible = msg.bible;
    poolPending.sourceTotal += msg.source.total;
    poolPending.sourceParts.push(msg.source.top);
    if (msg.timing) {
      poolPending.bibleMs = Math.max(poolPending.bibleMs, msg.timing.bibleMs);
      poolPending.maxSourceMs = Math.max(poolPending.maxSourceMs, msg.timing.sourceMs);
      Object.assign(poolPending.perSourceMs, msg.timing.perSourceMs);
    }
    settlePoolReply();
  }

  // A worker that throws (a bad Fuse.js edge case, an out-of-memory hiccup,
  // whatever) must still count toward completion — otherwise `remaining`
  // never reaches 0, searchInFlight is stuck true forever, and every search
  // after that point just queues up behind it and never fires again. Losing
  // that one worker's shard of results for this query is an acceptable
  // trade for "search still works at all."
  function handlePoolWorkerError(workerIndex, err) {
    console.warn(`TEP: search worker ${workerIndex} errored:`, err && err.message ? err.message : err);
    if (!poolPending) return; // no search currently in flight — nothing to unstick
    settlePoolReply();
  }

  function settlePoolReply() {
    poolPending.remaining--;
    if (poolPending.remaining > 0) return; // still waiting on the rest of the pool

    const merged = poolPending;
    poolPending = null;

    searchInFlight = false;
    if (queuedSearch) {
      const next = queuedSearch;
      queuedSearch = null;
      dispatchSearch(next.rawQuery, next.query, next.phase);
    }

    if (merged.reqId !== latestSearchReqId) return; // stale — a newer request has since superseded this response

    const roundTripMs = +(performance.now() - searchDispatchedAt).toFixed(1);
    console.log(`[render] "${merged.query}" (${merged.phase}) — round trip ${roundTripMs}ms across ${searchWorkers.length} workers (bible ${merged.bibleMs}ms, slowest source shard ${merged.maxSourceMs}ms)`, merged.perSourceMs);

    const mergedSource = {
      total: merged.sourceTotal,
      top: merged.sourceParts.flat().sort((a, b) => (a.score ?? 0) - (b.score ?? 0)).slice(0, MERGE_TOP_CAP)
    };
    finishSearchResult(merged.rawQuery, merged.query, merged.phase, merged.bible, mergedSource);
  }

  // Only reach for spelling correction if the literal query found nothing
  // anywhere (no claim entries, no Bible verses) — see finalizeRender for
  // where a successful/failed correction ends up rendered.
  function finishSearchResult(rawQuery, query, phase, bible, source) {
    if (phase === 'original') {
      const entries = entriesFor(query);
      if (entries.length === 0 && bible.total === 0) {
        const attempt = correctQuery(query);
        if (attempt.changed) {
          pendingOriginalSource = source;
          requestSearch(rawQuery, attempt.corrected, 'correction');
          return;
        }
      }
      finalizeRender(rawQuery, query, false, entries, bible, source);
      return;
    }

    // phase === 'correction'
    const entries = entriesFor(query);
    if (entries.length > 0 || bible.total > 0) {
      finalizeRender(rawQuery, query, true, entries, bible, source);
    } else {
      // Correction didn't help either — fall back to the original query's
      // own source-text matches (if any), which are still valid even though
      // nothing matched among claim entries or the Bible.
      finalizeRender(rawQuery, rawQuery, false, [], EMPTY_MATCHES, pendingOriginalSource);
    }
  }

  function finalizeRender(rawQuery, query, corrected, allMatchedEntries, bibleResult, sourceResult) {
    renderTopicOverview(query, allMatchedEntries, bibleResult.top);
    renderBibleResults(query, bibleResult);
    renderSourceResults(query, sourceResult);

    let matches = ENTRIES
      .map(e => ({ entry: e, s: score(e, query) }))
      .filter(m => m.s > 0)
      .filter(m => activeFilter === 'all' || m.entry.religion === activeFilter)
      .sort((a, b) => b.s - a.s)
      .map(m => m.entry);

    const correctionNote = corrected
      ? `<span class="correction-note">Showing results for "${escapeHtml(query)}"</span>`
      : '';

    if (matches.length === 0) {
      currentMatches = [];
      currentResultsPage = 0;
      resultsEl.innerHTML = '';
      renderResultsPagination();
      emptyEl.style.display = 'block';
      metaEl.innerHTML = correctionNote;
      return;
    }
    emptyEl.style.display = 'none';
    metaEl.innerHTML = rawQuery
      ? `${matches.length} match${matches.length === 1 ? '' : 'es'} for "${escapeHtml(query)}" ${correctionNote}`
      : `${matches.length} entries`;

    // A fresh search/filter always starts back on page 1, even if a stale
    // page number was left over from a previous, longer result set.
    currentMatches = matches;
    currentResultsPage = 0;
    renderResultsPage();

    if (rawQuery) {
      matches.slice(0, 5).forEach(entry => logSearchHit(entryId(entry)));
    }
  }

  // The actual Fuse.js search now runs off-thread in search-worker.js, so
  // typing can never be blocked by it. This debounce reduces how many
  // throwaway searches get dispatched during a typing burst — the in-flight
  // cap above is what guarantees no backlog, this just cuts how often it
  // needs to kick in.
  let renderDebounceTimer = null;
  function scheduleRender() {
    clearTimeout(renderDebounceTimer);
    renderDebounceTimer = setTimeout(render, 150);
  }
  searchInput.addEventListener('input', scheduleRender);

  // Repopulates the text-filter dropdown for whichever tradition is now
  // active. Hidden entirely for "All" and for any tradition with fewer than
  // two source texts (Islam's six hadith collections need it; Apocrypha's
  // single deuterocanon file doesn't — a dropdown with only one real choice
  // besides "All" is just noise).
  function updateTextFilterForTradition(tradition) {
    activeSourceId = null;
    if (!textFilterSelect) return;
    const matching = tradition === 'all' ? [] : SOURCE_TEXTS.filter(s => s.tradition === tradition);
    if (matching.length < 2) {
      textFilterSelect.style.display = 'none';
      textFilterSelect.innerHTML = '';
      return;
    }
    textFilterSelect.innerHTML = `<option value="">All ${escapeHtml(tradition)} texts</option>` +
      matching.map(s => `<option value="${escapeHtml(s.id)}">${escapeHtml(s.label)}</option>`).join('');
    textFilterSelect.value = '';
    textFilterSelect.style.display = '';
  }
  if (textFilterSelect) {
    textFilterSelect.addEventListener('change', () => {
      activeSourceId = textFilterSelect.value || null;
      render();
    });
  }

  chips.forEach(chip => {
    chip.addEventListener('click', () => {
      chips.forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      activeFilter = chip.dataset.filter;
      updateTextFilterForTradition(activeFilter);
      render();
    });
  });

  const religionSearch = document.getElementById('religion-search');
  const sidebarEmpty = document.getElementById('sidebar-empty');
  religionSearch.addEventListener('input', () => {
    const q = religionSearch.value.trim().toLowerCase();
    let anyVisible = false;
    chips.forEach(chip => {
      const isAll = chip.dataset.filter === 'all';
      const matches = isAll || chip.textContent.toLowerCase().includes(q);
      chip.style.display = matches ? '' : 'none';
      if (matches) anyVisible = true;
    });
    sidebarEmpty.style.display = anyVisible ? 'none' : 'block';
  });

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('./sw.js').catch(() => {});
    });
  }

  /* ================= Other traditions' source texts ================= */
  /*
   * Each source is fetched at runtime, cached in IndexedDB, and searched offline
   * alongside Scripture. Every source below is public domain or openly licensed —
   * no API keys, no redistribution restrictions.
   *
   * `urls` is a fallback chain: the first URL that responds and parses wins.
   * `parse` is deliberately defensive: it normalizes whatever shape comes back
   * into a flat [{ ref, text }] list. If a source fails entirely, the app skips
   * it and reports it in Settings — it never fails silently or breaks the app.
   */

  // Sutta titles for Digha Nikaya (1-34) and Majjhima Nikaya (1-152), used only
  // to label "Read Full Text" groups — the canonical English titles from
  // Sujato's translation, index 0 unused (suttas are numbered from 1).
  const DN_TITLES = [null, "The Divine Net","The Fruits of the Ascetic Life","With Ambaṭṭha","With Soṇadaṇḍa","With Kūṭadanta","With Mahāli","With Jāliya","The Lion’s Roar to the Naked Ascetic Kassapa","With Poṭṭhapāda","With Subha","With Kevaḍḍha","With Lohicca","Experts in the Three Vedas","The Great Discourse on Traces Left Behind","The Great Discourse on Causation","The Great Discourse on the Buddha’s Extinguishment","King Mahāsudassana","With Janavasabha","The Great Steward","The Great Congregation","Sakka’s Questions","The Longer Discourse on Mindfulness Meditation","With Pāyāsi","About Pāṭikaputta","The Lion’s Roar at the Monastery of Lady Udumbarikā","The Wheel-Turning Monarch","What Came First","Inspiring Confidence","An Impressive Discourse","The Marks of a Great Man","Advice to Sigālaka","The Āṭānāṭiya Protection","Reciting in Concert","Up to Ten"
  ];
  // Shared by the sn-an-highlights source (fetchRows) and parseReadRef,
  // so there's exactly one list to keep in sync, not two.
  const SN_AN_HIGHLIGHTS = [
    { path: 'sn/sn56/sn56.11', prefix: 'sn56.11', label: 'Samyutta Nikaya 56.11 (Setting the Wheel of Dhamma in Motion)' },
    { path: 'sn/sn22/sn22.59', prefix: 'sn22.59', label: 'Samyutta Nikaya 22.59 (The Characteristic of Non-Self)' },
    { path: 'sn/sn35/sn35.28', prefix: 'sn35.28', label: 'Samyutta Nikaya 35.28 (The Fire Sermon)' },
    { path: 'sn/sn12/sn12.2', prefix: 'sn12.2', label: 'Samyutta Nikaya 12.2 (Analysis of Dependent Origination)' },
    { path: 'sn/sn45/sn45.8', prefix: 'sn45.8', label: 'Samyutta Nikaya 45.8 (Analysis of the Noble Eightfold Path)' },
    { path: 'sn/sn56/sn56.31', prefix: 'sn56.31', label: 'Samyutta Nikaya 56.31 (The Simsapa Leaves)' },
    { path: 'an/an3/an3.65', prefix: 'an3.65', label: 'Anguttara Nikaya 3.65 (With the Kalamas)' }
  ];

  const MN_TITLES = [null, "The Root of All Things","All the Defilements","Heirs in the Teaching","Fear and Dread","Unblemished","One Might Wish","The Simile of the Cloth","Self-Effacement","Right View","Mindfulness Meditation","The Shorter Discourse on the Lion’s Roar","The Longer Discourse on the Lion’s Roar","The Longer Discourse on the Mass of Suffering","The Shorter Discourse on the Mass of Suffering","Measuring Up","Hard-heartedness","Jungle Thickets","The Honey-Cake","Two Kinds of Thought","How to Stop Thinking","The Simile of the Saw","The Simile of the Cobra","The Termite Mound","Chariots at the Ready","Sowing","The Noble Quest","The Shorter Simile of the Elephant’s Footprint","The Longer Simile of the Elephant’s Footprint","The Longer Simile of the Heartwood","The Shorter Simile of the Heartwood","The Shorter Discourse at Gosiṅga","The Longer Discourse at Gosiṅga","The Longer Discourse on the Cowherd","The Shorter Discourse on the Cowherd","The Shorter Discourse With Saccaka","The Longer Discourse With Saccaka","The Shorter Discourse on the Ending of Craving","The Longer Discourse on the Ending of Craving","The Longer Discourse at Assapura","The Shorter Discourse at Assapura","The People of Sālā","The People of Verañjā","The Great Elaboration","The Shorter Elaboration","The Shorter Discourse on Taking Up Practices","The Great Discourse on Taking Up Practices","The Inquirer","The Mendicants of Kosambī","On the Invitation of Divinity","The Condemnation of Māra","With Kandaraka","The Wealthy Citizen","A Trainee","With Potaliya the Householder","With Jīvaka","With Upāli","The Ascetic Who Behaved Like a Dog","With Prince Abhaya","The Many Kinds of Feeling","A Sure Bet","Advice to Rāhula at Ambalaṭṭhika","The Longer Advice to Rāhula","The Shorter Discourse With Māluṅkyaputta","The Longer Discourse With Māluṅkya","With Bhaddāli","The Simile of the Quail","At Cātumā","At Naḷakapāna","With Gulissāni","At Kīṭāgiri","To Vacchagotta on the Three Knowledges","With Vacchagotta on Fire","The Longer Discourse With Vacchagotta","With Dīghanakha","With Māgaṇḍiya","With Sandaka","The Longer Discourse with Sakuludāyī","With Uggāhamāna Samaṇamaṇḍikāputta","The Shorter Discourse With Sakuludāyī","With Vekhanasa","With Ghaṭīkāra","With Raṭṭhapāla","About King Maghadeva","At Madhurā","With Prince Bodhi","With Aṅgulimāla","Born From the Beloved","The Imported Cloth","Shrines to the Teaching","At Kaṇṇakatthala","With Brahmāyu","With Sela","With Assalāyana","With Ghoṭamukha","With Caṅkī","With Esukārī","With Dhanañjāni","With Vāseṭṭha","With Subha","With Saṅgārava","At Devadaha","The Five and Three","Is This What You Think Of Me?","At Sāmagāma","With Sunakkhatta","Conducive to the Imperturbable","With Moggallāna the Accountant","With Moggallāna the Guardian","The Longer Discourse on the Full-Moon Night","The Shorter Discourse on the Full-Moon Night","One by One","The Sixfold Purification","A True Person","What Should and Should Not Be Cultivated","Many Elements","At Isigili","The Great Forty","Mindfulness of Breathing","Mindfulness of the Body","Rebirth by Choice","The Shorter Discourse on Emptiness","The Longer Discourse on Emptiness","Incredible and Amazing","With Bakkula","The Level of the Tamed","With Bhūmija","With Anuruddha","Corruptions","The Foolish and the Astute","Messengers of the Gods","One Fine Night","Ānanda and One Fine Night","Mahākaccāna and One Fine Night","Lomasakaṅgiya and One Fine Night","The Shorter Analysis of Deeds","The Longer Analysis of Deeds","The Analysis of the Six Sense Fields","A Summary Recital and its Analysis","The Analysis of No Strife","The Analysis of the Elements","The Analysis of the Truths","The Analysis of Religious Donations","Advice to Anāthapiṇḍika","Advice to Channa","Advice to Puṇṇa","Advice from Nandaka","The Shorter Advice to Rāhula","Six By Six","The Great Discourse on What Relates to the Six Sense Fields","With the People of Nagaravinda","The Purification of Alms","The Development of the Faculties"
  ];

  const SOURCE_TEXTS = [
    {
      id: 'quran',
      label: "Qur'an",
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/quran-api',
      /*
       * The editions index is an object keyed by edition id:
       *   { "eng-abdullahyusufal": { name, author, language: "English", link, linkmin }, ... }
       * It hands us the real file URL in `link`/`linkmin`, so we use that rather than
       * building a URL ourselves — no guessing at translation slugs.
       */
      discover: async () => {
        const INDEX_URLS = [
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions.min.json',
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions.json'
        ];

        for (const indexUrl of INDEX_URLS) {
          try {
            const res = await fetch(indexUrl);
            if (!res.ok) continue;
            const editions = await res.json();
            const list = Array.isArray(editions) ? editions : Object.values(editions);

            // Plain English editions only — skip latin-script transliterations (-la / -lad).
            const english = list.filter(e =>
              e && typeof e === 'object' &&
              typeof e.language === 'string' &&
              e.language.trim().toLowerCase() === 'english' &&
              typeof e.name === 'string' &&
              !/-la(d)?$/.test(e.name)
            );
            if (!english.length) continue;

            // Prefer well-known public-domain translations, else take the first English one.
            const preferred = ['eng-abdullahyusufal', 'eng-mohammadhabibsh', 'eng-mohammedmarmadu'];
            const pick = english.find(e => preferred.includes(e.name)) || english[0];

            const urls = [pick.linkmin, pick.link].filter(u => typeof u === 'string' && u);
            if (urls.length) return urls;
          } catch (err) {
            // try next index URL
          }
        }

        // Last resort if the index itself is unreachable.
        return [
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/eng-abdullahyusufal.min.json',
          'https://cdn.jsdelivr.net/gh/fawazahmed0/quran-api@1/editions/eng-abdullahyusufal.json'
        ];
      },
      parse: (data) => normalizeVerseList(data, (row, i) => {
        const ch = row.chapter ?? row.surah ?? row.sura;
        const vs = row.verse ?? row.ayah ?? row.aya;
        return (ch != null && vs != null) ? `Qur'an ${ch}:${vs}` : `Qur'an ${i + 1}`;
      }, { requireLatin: true })
    },
    {
      id: 'gita',
      label: 'Bhagavad Gita',
      tradition: 'Hinduism',
      license: 'Unlicense (public domain dedication) — gita/gita',
      /*
       * VERIFIED: data/verse.json exists and returns a bare array whose rows carry
       * chapter_id / chapter_number. In this repo the verse rows hold the Sanskrit,
       * while English lives in a sibling translation.json joined by verse id — so we
       * fetch both and stitch them. If translation.json is missing or shaped
       * differently, we fall back to any Latin-script text already on the verse row.
       */
      fetchRows: async () => {
        const BASE = 'https://cdn.jsdelivr.net/gh/gita/gita@c6fce39595445768876ddbb8d1268a9c935e1d2b/data';

        const getJson = async (url) => {
          const res = await fetch(url);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json();
        };

        const asArray = (d) => {
          if (Array.isArray(d)) return d;
          if (d && typeof d === 'object') {
            for (const v of Object.values(d)) {
              if (Array.isArray(v) && v.length && typeof v[0] === 'object') return v;
            }
          }
          return [];
        };

        const verses = asArray(await getJson(`${BASE}/verse.json`));
        if (!verses.length) throw new Error('verse.json returned no rows');

        // Try to pull English translations and index them by verse id.
        const englishByVerseId = {};
        try {
          const translations = asArray(await getJson(`${BASE}/translation.json`));
          translations.forEach(t => {
            if (!t || typeof t !== 'object') return;
            const lang = String(t.lang ?? t.language ?? '').toLowerCase();
            if (lang && !lang.startsWith('en')) return;   // English only
            const vid = t.verse_id ?? t.verseId ?? t.verse ?? t.id;
            if (vid == null) return;
            const text = t.description ?? t.translation ?? t.text ?? t.content;
            if (typeof text !== 'string' || !text.trim()) return;
            // Keep the first English translation we see per verse.
            if (!englishByVerseId[vid]) englishByVerseId[vid] = text.trim();
          });
        } catch (err) {
          // translation.json unavailable — fall through to verse-row text below.
        }

        const isMostlyLatin = (s) => {
          const letters = s.replace(/[^A-Za-z\u0900-\u097F\u0600-\u06FF]/g, '');
          if (!letters.length) return false;
          return (letters.match(/[A-Za-z]/g) || []).length / letters.length > 0.6;
        };

        const rows = verses.map((v) => {
          const ch = v.chapter_number ?? v.chapter_id ?? v.chapter;
          const vs = v.verse_number ?? v.verse_order ?? v.verse;
          const vid = v.id ?? v.verse_id;

          let text = (vid != null && englishByVerseId[vid]) || null;

          // Fallback: an English field already present on the verse row.
          if (!text) {
            for (const k of ['translation', 'english', 'meaning', 'text']) {
              const val = v[k];
              if (typeof val === 'string' && val.trim() && isMostlyLatin(val)) { text = val.trim(); break; }
            }
          }
          if (!text) return null;

          const ref = (ch != null && vs != null)
            ? `Bhagavad Gita ${ch}:${vs}`
            : `Bhagavad Gita ${vid ?? '?'}`;
          return { ref, text: stripHtml(text) };
        }).filter(Boolean);

        if (!rows.length) throw new Error('no English text found (verse.json is Sanskrit; translation.json join failed)');
        return rows;
      }
    },
    /*
     * Yoga Sutras of Patanjali — Charles Johnston's 1912 translation,
     * public domain. 195 sutras across the 4 traditional padas. Only the
     * translated sutra text is kept, not Johnston's verse-by-verse mystical
     * commentary.
     */
    {
      id: 'yoga-sutras',
      label: 'Yoga Sutras of Patanjali',
      tradition: 'Hinduism',
      license: 'Public domain — Charles Johnston translation (1912)',
      urls: ['./yoga-sutras.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Brahma Sutras — George Thibaut's translation (Sacred Books of the
     * East vols. 34 and 38, Shankara's commentary), public domain. Only
     * each sutra's own one-sentence translation is kept, not the
     * surrounding paragraphs of Shankara's commentary/disputation (same
     * principle as Summa Theologica below — keep the core statement, not
     * the argument around it). 545 of the traditional ~555 sutras are
     * present; the remainder could not be cleanly isolated from
     * surrounding commentary in the source scan (severe OCR corruption of
     * the sutra number itself in a handful of spots) and are simply
     * absent rather than guessed at.
     */
    {
      id: 'brahma-sutras',
      label: 'Brahma Sutras',
      tradition: 'Hinduism',
      license: 'Public domain — George Thibaut translation, Sacred Books of the East vols. 34 & 38',
      urls: ['./brahma-sutras.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Mahabharata — Kisari Mohan Ganguli's prose translation
     * (1883-96), the only complete English translation in the public
     * domain, via sacred-texts.com's Distributed Proofreaders text
     * (mirrored at github.com/aasi-archive/mbh). One row per numbered
     * section (adhyaya) rather than per verse — Ganguli's translation is
     * continuous prose, not individually verse-numbered like the Gita.
     * 2,110 sections across all 18 parvas.
     */
    {
      id: 'mahabharata',
      label: 'The Mahabharata',
      tradition: 'Hinduism',
      license: 'Public domain — Kisari Mohan Ganguli translation (1883-96)',
      urls: ['./mahabharata.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The 13 principal Upanishads — Robert Ernest Hume's 1921 translation
     * ("The Thirteen Principal Upanishads", Oxford University Press),
     * public domain — the one translation covering exactly this canonical
     * set of 13 in one voice, including Mandukya (which Max Müller's
     * earlier SBE edition omits). Verses are numbered as one flat
     * continuous count per Upanishad rather than reconstructing the full
     * traditional adhyaya/brahmana/valli/khanda nesting (which varies by
     * text and isn't consistently machine-derivable from this scan) —
     * same simplification this app already uses for the Dhammapada,
     * which also doesn't preserve its traditional vagga divisions.
     */
    {
      id: 'upanishads',
      label: 'The Principal Upanishads',
      tradition: 'Hinduism',
      license: 'Public domain — Robert Ernest Hume translation (1921)',
      urls: ['./upanishads.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Ramayana — Ralph T.H. Griffith's verse translation (1870-74),
     * public domain, via Project Gutenberg. One row per Canto (Griffith's
     * translation is continuous verse, not individually shloka-numbered).
     * Covers Books I-VI (Bala, Ayodhya, Aranya, Kishkindha, Sundara,
     * Yuddha) — the six books universally attributed to the core epic;
     * Griffith's own translation does not include Uttara Kanda, which is
     * widely regarded by scholars as a later addition to the text, so
     * it's absent here rather than patched in from a different translator
     * (which would mix two voices in what should read as one text). A
     * handful of individual canto numbers (e.g. 55-58 in Yuddha Kanda) are
     * also absent from Griffith's own numbering, not lost in extraction —
     * he kept the traditional numbering for scholarly cross-reference
     * even where he omitted a passage as a probable later interpolation.
     */
    {
      id: 'ramayana',
      label: 'The Ramayana',
      tradition: 'Hinduism',
      license: 'Public domain — Ralph T.H. Griffith translation (1870-74)',
      urls: ['./ramayana.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Vishnu Purana — Manmatha Nath Dutt's translation (1894-96), based on
     * H.H. Wilson's earlier work, public domain. One row per numbered
     * Section (continuous prose, not verse-numbered), grouped under 6
     * traditional Books.
     */
    {
      id: 'vishnu-purana',
      label: 'Vishnu Purana',
      tradition: 'Hinduism',
      license: 'Public domain — Manmatha Nath Dutt translation (1894-96), after H.H. Wilson',
      urls: ['./vishnu-purana.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Manusmriti ("The Laws of Manu") — George Bühler's 1886 translation
     * (Sacred Books of the East vol. 25), public domain, complete, all 12
     * chapters. Verse 6:76-77 is preserved as a single combined row because
     * Bühler's own translation merges those two verses into one continuous
     * rendering — not a gap in extraction.
     */
    {
      id: 'manusmriti',
      label: 'Manusmriti (Laws of Manu)',
      tradition: 'Hinduism',
      license: 'Public domain — George Bühler translation (1886), Sacred Books of the East vol. 25',
      urls: ['./manusmriti.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    {
      id: 'dhammapada',
      label: 'Dhammapada',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      /*
       * Fetches the plain-English translation files directly from SuttaCentral's
       * own bilara-data repo on GitHub (published branch), instead of their
       * `/api/bilarasuttas/...` endpoint. That API returns several parallel
       * segment maps for the same sutta in one payload — root-language (Pali,
       * itself in Latin transliteration so it can't be told apart from English
       * by script alone), an HTML-templated variant, and the plain translation —
       * with no reliable way to tell which map is which from content alone,
       * which is what caused HTML/placeholder markup to show up as "code"
       * instead of readable text. These files contain only the English
       * translation, so there's nothing left to disambiguate.
       */
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/dhp';
        const VAGGAS = [
          'dhp1-20','dhp21-32','dhp33-43','dhp44-59','dhp60-75','dhp76-89','dhp90-99',
          'dhp100-115','dhp116-128','dhp129-145','dhp146-156','dhp157-166','dhp167-178',
          'dhp179-196','dhp197-208','dhp209-220','dhp221-234','dhp235-255','dhp256-272',
          'dhp273-289','dhp290-305','dhp306-319','dhp320-333','dhp334-359','dhp360-382',
          'dhp383-423'
        ];
        const chunks = await Promise.all(VAGGAS.map(async (uid) => {
          try {
            const res = await fetch(`${BASE}/${uid}_translation-en-sujato.json`);
            if (!res.ok) return [];
            const data = await res.json();
            const byVerse = {};
            Object.entries(data).forEach(([segId, text]) => {
              const m = segId.match(/^(dhp\d+)[:.](\d+)/i);
              if (!m) return;
              // Segments numbered 0.x are chapter headings/titles, not verse content.
              if (m[2] === '0') return;
              const clean = stripHtml(String(text)).trim();
              if (!clean) return;
              const key = m[1].toLowerCase();
              (byVerse[key] = byVerse[key] || []).push(clean);
            });
            return Object.entries(byVerse).map(([key, lines]) => ({
              ref: `Dhammapada ${key.replace(/^dhp/i, '')}`,
              text: lines.join(' ').replace(/\s+/g, ' ').trim()
            })).filter(r => r.text.length > 0);
          } catch (err) {
            return [];
          }
        }));
        return chunks.flat();
      }
    },
    /*
     * Digha Nikaya (Long Discourses) and Majjhima Nikaya (Middle Discourses) —
     * the two most-cited collections in the Pali Canon's Sutta Pitaka, same
     * translator/license/repo as Dhammapada above. Both use a clean numeric
     * range with no gaps (verified against the repo's file listing), so they
     * share the fetchSujatoNikaya() routine defined near stripHtml() instead
     * of needing Dhammapada's hardcoded chunk list.
     */
    {
      id: 'digha-nikaya',
      label: 'Digha Nikaya (Long Discourses)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: () => fetchSujatoNikaya('dn', 34, 'Digha Nikaya')
    },
    {
      id: 'majjhima-nikaya',
      label: 'Majjhima Nikaya (Middle Discourses)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: () => fetchSujatoNikaya('mn', 152, 'Majjhima Nikaya')
    },
    /*
     * Sutta Nipata — one of the oldest strata of the Pali Canon (Khuddaka
     * Nikaya), 5 vaggas with a clean per-vagga file count (verified against
     * the repo's file listing: 12,14,12,16,19). Same translator/license/repo.
     */
    {
      id: 'sutta-nipata',
      label: 'Sutta Nipata',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/snp';
        const VAGGA_COUNTS = [12, 14, 12, 16, 19];
        const targets = [];
        VAGGA_COUNTS.forEach((count, vi) => {
          for (let n = 1; n <= count; n++) targets.push({ vagga: vi + 1, n });
        });
        const rows = await Promise.all(targets.map(async ({ vagga, n }) => {
          const url = `${BASE}/vagga${vagga}/snp${vagga}.${n}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Sutta Nipata ${vagga}.${n}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Udana ("Heartfelt Sayings") — Khuddaka Nikaya, 8 vaggas of exactly 10
     * suttas each (verified against the repo's file listing).
     */
    {
      id: 'udana',
      label: 'Udana',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/ud';
        const targets = [];
        for (let vagga = 1; vagga <= 8; vagga++) {
          for (let n = 1; n <= 10; n++) targets.push({ vagga, n });
        }
        const rows = await Promise.all(targets.map(async ({ vagga, n }) => {
          const url = `${BASE}/vagga${vagga}/ud${vagga}.${n}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Udana ${vagga}.${n}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Itivuttaka ("This Was Said") — Khuddaka Nikaya, 112 short suttas with
     * flat global numbering (iti1-iti112) but stored across 11 vagga folders;
     * ITI_VAGGA_BOUNDS below is the upper sutta number of each vagga,
     * verified against the repo's file listing.
     */
    {
      id: 'itivuttaka',
      label: 'Itivuttaka',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/kn/iti';
        const ITI_VAGGA_BOUNDS = [10, 20, 27, 37, 49, 59, 69, 79, 89, 99, 112];
        const vaggaFor = (num) => {
          for (let v = 0; v < ITI_VAGGA_BOUNDS.length; v++) {
            if (num <= ITI_VAGGA_BOUNDS[v]) return v + 1;
          }
          return ITI_VAGGA_BOUNDS.length;
        };
        const rows = await Promise.all(Array.from({ length: 112 }, (_, i) => i + 1).map(async (num) => {
          const vagga = vaggaFor(num);
          const url = `${BASE}/vagga${vagga}/iti${num}_translation-en-sujato.json`;
          const text = await fetchSujatoVerse(url);
          return text ? { ref: `Itivuttaka ${num}`, text } : null;
        }));
        return rows.filter(Boolean);
      }
    },
    /*
     * Selected Suttas — a hand-picked set of the single most iconic suttas
     * from the Samyutta and Anguttara Nikayas: the first sermon (Four Noble
     * Truths, Middle Way), the second sermon (non-self), the Fire Sermon
     * (third major sermon), dependent origination and the noble eightfold
     * path formally defined, the "leaves in the hand" simile on the limited
     * scope of what the Buddha taught, and the Kalama Sutta on not accepting
     * claims on authority alone. These two Nikayas together hold thousands of
     * suttas split across ~3,200 small, irregularly-ranged files (an1-an11,
     * sn1-sn56 each subdivided further) — fetching the full corpus the way
     * Digha/Majjhima are above was tried and rejected as impractical (way
     * more requests than the rest of this app's sources combined, and no
     * clean sequential numbering to generate the fetch list from). This list
     * covers what a reader is actually likely to look for from these two
     * collections without that cost.
     */
    {
      id: 'sn-an-highlights',
      label: 'Selected Suttas (Samyutta & Anguttara Nikayas)',
      tradition: 'Buddhism',
      license: 'CC0 public domain dedication — Bhikkhu Sujato / SuttaCentral',
      fetchRows: async () => {
        const results = await Promise.all(SN_AN_HIGHLIGHTS.map(t => fetchSujatoSutta(t.path, t.prefix, t.label)));
        return results.flat();
      }
    },
    /*
     * Heart Sutra — both recensions (Larger, with its narrative frame, and
     * Smaller, the version normally chanted), from Max Muller's 1894
     * translation (Sacred Books of the East vol. 49). Hand-corrected against
     * the archive.org OCR scan (buddhistmahy02cowe): the underlying prose is
     * clean, but Muller's Sanskrit transliteration diacritics (e.g.
     * "Pra^;7aparamita", "^'ariputra") don't survive OCR at all, so proper
     * nouns and technical terms were manually restored to plain spelling
     * (Prajnaparamita, Shariputra, Bhagavat, Nirvana, etc.) — this is the
     * one source this session that's typed by hand rather than scripted,
     * since at ~700 words total it was faster and more reliable than writing
     * a cleanup pass for a two-paragraph text.
     */
    {
      id: 'heart-sutra',
      label: 'Heart Sutra',
      tradition: 'Buddhism',
      license: 'Public domain — F. Max Muller translation (1894), Sacred Books of the East vol. 49',
      urls: ['./heart-sutra.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The six canonical Sunni hadith collections (Kutub al-Sittah), from the
     * same author/project as the Qur'an source above (fawazahmed0), same
     * Unlicense terms. Each collection is one large JSON file — verified
     * shape: { metadata, hadiths: [{ hadithnumber, text, reference: {book,
     * hadith} }] } — so no discovery or per-chunk fetching is needed, unlike
     * Dhammapada above.
     */
    {
      id: 'hadith-bukhari',
      label: 'Sahih al-Bukhari',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-bukhari.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sahih al-Bukhari ${ref.book}:${ref.hadith}`
          : `Sahih al-Bukhari #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-muslim',
      label: 'Sahih Muslim',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-muslim.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sahih Muslim ${ref.book}:${ref.hadith}`
          : `Sahih Muslim #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-abudawud',
      label: 'Sunan Abu Dawud',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-abudawud.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan Abu Dawud ${ref.book}:${ref.hadith}`
          : `Sunan Abu Dawud #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-tirmidhi',
      label: 'Jami at-Tirmidhi',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-tirmidhi.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Jami at-Tirmidhi ${ref.book}:${ref.hadith}`
          : `Jami at-Tirmidhi #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-nasai',
      label: "Sunan an-Nasa'i",
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-nasai.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan an-Nasa'i ${ref.book}:${ref.hadith}`
          : `Sunan an-Nasa'i #${row.hadithnumber}`;
      })
    },
    {
      id: 'hadith-ibnmajah',
      label: 'Sunan Ibn Majah',
      tradition: 'Islam',
      license: 'Unlicense (public domain dedication) — fawazahmed0/hadith-api',
      urls: ['https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/eng-ibnmajah.min.json'],
      parse: (data) => normalizeVerseList(data, (row) => {
        const ref = row.reference;
        return (ref && ref.book != null && ref.hadith != null)
          ? `Sunan Ibn Majah ${ref.book}:${ref.hadith}`
          : `Sunan Ibn Majah #${row.hadithnumber}`;
      })
    },
    /*
     * Vendored locally (same reason as fuse.min.js — no dependency on a
     * third party staying up) rather than fetched cross-origin.
     * baltimore-catechism.json is a cleaned Q/A extraction of "An
     * Explanation of the Baltimore Catechism of Christian Doctrine" (Rev.
     * Thomas L. Kinkead, "Baltimore Catechism No. 4", imprimatur 1891/1921)
     * from Project Gutenberg ebook #14554 — confirmed public domain, unlike
     * the modern Catechism of the Catholic Church (1992), which remains
     * under copyright. It predates a few dogmas defined after 1921 (e.g.
     * the Assumption, 1950), so it's an older but still authoritative and
     * doctrinally representative Catholic catechism, not the current one.
     */
    {
      id: 'baltimore-catechism',
      label: 'Baltimore Catechism',
      tradition: 'Catholicism',
      license: 'Public domain — Project Gutenberg ebook #14554',
      urls: ['./baltimore-catechism.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({
            ref: `Baltimore Catechism, Q. ${row.n}`,
            text: stripHtml(`Q. ${row.q} A. ${row.a}`)
          }))
        : []
    },
    /*
     * The seven deuterocanonical Old Testament books, a.k.a. the Apocrypha —
     * kept as their own tradition rather than folded into "Catholicism",
     * since they're not a distinctively Catholic composition: the Orthodox
     * churches also hold them as canonical, and they were printed in early
     * Protestant Bibles (including the original 1611 KJV) as a separate
     * section, before falling out of common Protestant use. Douay-Rheims
     * (Challoner revision, mid-18th century) is the traditional English
     * Catholic translation and is public domain (pre-1923).
     *
     * Vendored locally rather than fetched from a third-party GitHub JSON
     * (xxruyle/Bible-DouayRheims) after that source turned out to have a
     * real data bug: its "2 Machabees" is missing chapter 7 entirely (the
     * martyrdom of the seven brothers), silently shifting every later verse
     * back by one chapter under the wrong number. deuterocanon.json here was
     * extracted directly from Project Gutenberg ebook #1581 instead — its
     * chapter counts for all seven books were verified against the standard
     * Douay-Rheims structure (no gaps), and Challoner's inline footnotes
     * (appended to verse text as "...."-delimited asides in that edition)
     * were stripped so only the actual verse text remains.
     */
    {
      id: 'deuterocanon',
      label: 'Apocrypha / Deuterocanon (Douay-Rheims)',
      tradition: 'Apocrypha',
      license: 'Public domain (pre-1923) — Douay-Rheims, Challoner revision, via Project Gutenberg ebook #1581',
      urls: ['./deuterocanon.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The 126 dogmatic canons of the Council of Trent (1545-1563) — the
     * "if any one saith X, let him be anathema" statements on Justification,
     * the Sacraments, the Eucharist, Penance, Order, Matrimony, etc. Public
     * domain (J. Waterworth's 1848 translation). Vendored from a single-page
     * transcription hosted by the Hanover Historical Texts Project rather
     * than fetched live — it's plain HTML on an institutional site with no
     * CORS headers for cross-origin fetch, and no JSON version exists.
     *
     * That transcription has a handful of real typos (e.g. "CANON lI." and
     * "CANON 11." for "CANON II.", "let him be be anathema" for "let him be
     * anathema") which were corrected during extraction — verified by
     * checking the canon count for every session/topic against the
     * unmodified source (all 126 accounted for, none merged or dropped).
     *
     * Decrees (as opposed to canons) are not included: Trent's canons are
     * short, numbered, and self-contained ("if anyone says X, anathema"),
     * which is what makes them directly comparable entries; the decrees are
     * long discursive prose better suited to reading in full than to
     * verse-style search results.
     */
    {
      id: 'trent-canons',
      label: 'Council of Trent — Canons',
      tradition: 'Catholicism',
      license: 'Public domain — J. Waterworth translation (1848), via Hanover Historical Texts Project',
      urls: ['./trent-canons.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * Two pre-1929 papal encyclicals, both firmly public domain by age —
     * Rerum Novarum (Leo XIII, 1891, the foundational document of Catholic
     * social teaching) and Ineffabilis Deus (Pius IX, 1854, the dogmatic
     * definition of the Immaculate Conception, already cited in the CCC-era
     * entries above via the Baltimore Catechism's summary of it). Vendored
     * from papalencyclicals.net rather than fetched live (no CORS headers,
     * and no JSON version exists). That site doesn't number every paragraph
     * consistently across the whole document, so paragraphs are numbered
     * sequentially here rather than reusing the site's own inline numbers —
     * a stable, collision-free citation was judged more important than
     * matching another site's paragraph numbering exactly.
     */
    {
      id: 'rerum-novarum',
      label: 'Rerum Novarum (Leo XIII, 1891)',
      tradition: 'Catholicism',
      license: 'Public domain (pre-1929)',
      urls: ['./rerum-novarum.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    {
      id: 'ineffabilis-deus',
      label: 'Ineffabilis Deus (Pius IX, 1854)',
      tradition: 'Catholicism',
      license: 'Public domain (pre-1929)',
      urls: ['./ineffabilis-deus.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    },
    /*
     * The Summa Theologica (St. Thomas Aquinas, 13th c.) — the single most
     * influential work of Catholic systematic theology, and public domain
     * (Fathers of the English Dominican Province translation, 1920, via
     * Project Gutenberg ebooks #17611/17897/18755/19950 for Parts I, I-II,
     * II-II, and III respectively). 2,637 articles total.
     *
     * Each article's Objections and Replies are structured disputation, not
     * Aquinas's own position — omitted for the same reason Trent's decrees
     * were skipped (see above), keeping only the "I answer that..." section,
     * which is the article's actual teaching and is what gets cited in
     * practice.
     *
     * The source text itself has a handful of numbering slips (an article
     * bracket-tagged with the same number as its neighbor, or labeled with
     * the wrong Part letter) — verified by cross-checking against the
     * spelled-out ordinal ("FIRST ARTICLE", "SECOND ARTICLE", etc.), which
     * never skips or repeats. Article numbers here are assigned by document
     * order within each Question rather than trusting either the bracket or
     * the ordinal word alone, since a few even had the ordinal word itself
     * duplicated. A handful of articles (5 out of 2,642 in Part I alone)
     * lack an extractable "I answer that" in this particular transcription
     * and are simply absent rather than guessed at.
     */
    {
      id: 'summa-theologica',
      label: 'Summa Theologica (Aquinas)',
      tradition: 'Catholicism',
      license: 'Public domain — Fathers of the English Dominican Province translation (1920), via Project Gutenberg',
      urls: ['./summa-theologica.json'],
      parse: (data) => Array.isArray(data)
        ? data.map((row) => ({ ref: row.ref, text: stripHtml(row.text) }))
        : []
    }
  ];

  function stripHtml(str) {
    return str
      .replace(/<[^>]*>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&#0*39;/gi, "'")
      .replace(/\s+/g, ' ')
      .trim();
  }

  /*
   * Shared fetch routine for whole-Nikaya SuttaCentral sources (Digha and
   * Majjhima) — each sutta is one file, numbered sequentially with no gaps,
   * so unlike Dhammapada above no hardcoded chunk list is needed. Segment ids
   * look like "dn1:1.1.1" (sutta:section.paragraph.sentence, depth varies by
   * sutta — some suttas add an extra top-level "chapter" number, e.g. long
   * suttas like DN 16). Rather than assume a fixed depth, every segment is
   * grouped with its siblings by dropping only the last (sentence-level)
   * component, then groups are numbered sequentially in file order — this
   * produces one row per paragraph regardless of how deeply that sutta happens
   * to be subdivided. Segments under "0" (nikaya/sutta title) are headings,
   * not body text, and are skipped — same convention as Dhammapada's "0.x".
   */
  // Splits one bilara segment map into ordered paragraph-level text blocks —
  // groups every segment by its id with the last (sentence-level) component
  // dropped, in first-appearance order. `idPrefix` is everything before the
  // colon in that file's segment ids (e.g. "dn1", or "sn56.11" for a Samyutta
  // sutta, whose own number already contains a dot).
  function groupSujatoSegments(data, idPrefix) {
    const escaped = idPrefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`^${escaped}:([\\d.]+)$`);
    const groupOrder = [];
    const groupLines = {};
    Object.keys(data).forEach((segId) => {
      const m = segId.match(re);
      if (!m) return;
      const parts = m[1].split('.');
      if (parts[0] === '0') return;
      const groupKey = parts.length > 1 ? parts.slice(0, -1).join('.') : parts[0];
      const clean = stripHtml(String(data[segId])).trim();
      if (!clean) return;
      if (!(groupKey in groupLines)) {
        groupLines[groupKey] = [];
        groupOrder.push(groupKey);
      }
      groupLines[groupKey].push(clean);
    });
    return groupOrder
      .map((key) => groupLines[key].join(' ').replace(/\s+/g, ' ').trim())
      .filter((text) => text.length > 0);
  }

  async function fetchSujatoNikaya(abbrev, count, label) {
    const BASE = `https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta/${abbrev}`;
    const nums = Array.from({ length: count }, (_, i) => i + 1);
    const chunks = await Promise.all(nums.map(async (n) => {
      try {
        const res = await fetch(`${BASE}/${abbrev}${n}_translation-en-sujato.json`);
        if (!res.ok) return [];
        const data = await res.json();
        return groupSujatoSegments(data, `${abbrev}${n}`).map((text, i) => ({
          ref: `${label} ${n}.${i + 1}`,
          text
        }));
      } catch (err) {
        return [];
      }
    }));
    return chunks.flat();
  }

  // Fetches one specific SuttaCentral file at `path` (relative to the sujato
  // sutta directory) and splits it into paragraph rows via groupSujatoSegments,
  // for the curated Samyutta/Anguttara highlights list — those Nikayas are
  // chunked into thousands of small irregularly-ranged files (verified via
  // the repo's file tree), too many and too fragile to fetch a full range of
  // like Digha/Majjhima above, so only a hand-picked list of the single most
  // iconic suttas is fetched, each by its known exact file path.
  async function fetchSujatoSutta(path, idPrefix, label) {
    const BASE = 'https://raw.githubusercontent.com/suttacentral/bilara-data/80641fa4c579b4a49d7ec3e5c627cd606d498cba/translation/en/sujato/sutta';
    try {
      const res = await fetch(`${BASE}/${path}_translation-en-sujato.json`);
      if (!res.ok) return [];
      const data = await res.json();
      return groupSujatoSegments(data, idPrefix).map((text, i) => ({
        ref: `${label} ${i + 1}`,
        text
      }));
    } catch (err) {
      return [];
    }
  }

  /*
   * Fetches one SuttaCentral/Sujato file that is itself a single short sutta
   * (Sutta Nipata, Udana, Itivuttaka verses) rather than a long discourse —
   * unlike fetchSujatoNikaya above, there's no internal paragraph structure
   * worth splitting on, so every non-"0.x" segment in the file is just
   * concatenated into one row's text, the same way Dhammapada's verses work.
   */
  async function fetchSujatoVerse(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      const data = await res.json();
      const lines = [];
      Object.keys(data).forEach((segId) => {
        // segId's own prefix can itself contain a dot (e.g. "snp1.1:0.1"),
        // so split on the LAST colon rather than assuming a bare \w+ prefix.
        const m = segId.match(/:([\d.]+)$/);
        if (!m) return;
        if (m[1].split('.')[0] === '0') return;
        const clean = stripHtml(String(data[segId])).trim();
        if (clean) lines.push(clean);
      });
      const text = lines.join(' ').replace(/\s+/g, ' ').trim();
      return text || null;
    } catch (err) {
      return null;
    }
  }

  /*
   * Turns an arbitrary JSON payload into a flat [{ref, text}] list.
   * Handles: a bare array, a wrapper object ({quran: [...]}, {verses: [...]}, etc.),
   * and picks whichever text-bearing field actually exists on each row.
   */
  function normalizeVerseList(data, makeRef, opts) {
    const options = opts || {};
    let rows = null;

    if (Array.isArray(data)) {
      rows = data;
    } else if (data && typeof data === 'object') {
      // Find the first array-of-objects property in the payload.
      for (const val of Object.values(data)) {
        if (Array.isArray(val) && val.length && typeof val[0] === 'object') { rows = val; break; }
      }
    }
    if (!rows) return [];

    const TEXT_KEYS = ['translation', 'english', 'englishTranslation', 'et', 'text', 'description', 'meaning', 'content', 'verse_text'];

    // Is this string mostly Latin letters? Guards against indexing Devanagari /
    // Arabic script when we want the English translation field instead.
    const isMostlyLatin = (s) => {
      const letters = s.replace(/[^A-Za-z\u0080-\u024F\u0900-\u097F\u0600-\u06FF]/g, '');
      if (!letters.length) return false;
      const latin = (letters.match(/[A-Za-z]/g) || []).length;
      return latin / letters.length > 0.6;
    };

    return rows.map((row, i) => {
      if (!row || typeof row !== 'object') return null;
      let text = null;
      for (const k of TEXT_KEYS) {
        const val = row[k];
        if (typeof val === 'string' && val.trim().length > 0) {
          if (options.requireLatin && !isMostlyLatin(val)) continue;
          text = val.trim();
          break;
        }
      }
      if (!text) return null;
      return { ref: makeRef(row, i), text: stripHtml(text) };
    }).filter(Boolean);
  }

  const BIBLE_BOOKS = [
    "1Chronicles","1Corinthians","1John","1Kings","1Peter","1Samuel","1Thessalonians","1Timothy",
    "2Chronicles","2Corinthians","2John","2Kings","2Peter","2Samuel","2Thessalonians","2Timothy",
    "3John","Acts","Amos","Colossians","Daniel","Deuteronomy","Ecclesiastes","Ephesians","Esther",
    "Exodus","Ezekiel","Ezra","Galatians","Genesis","Habakkuk","Haggai","Hebrews","Hosea","Isaiah",
    "James","Jeremiah","Job","Joel","John","Jonah","Joshua","Jude","Judges","Lamentations","Leviticus",
    "Luke","Malachi","Mark","Matthew","Micah","Nahum","Nehemiah","Numbers","Obadiah","Philemon",
    "Philippians","Proverbs","Psalms","Revelation","Romans","Ruth","SongofSolomon","Titus","Zechariah","Zephaniah"
  ];

  const DB_NAME = 'tep-bible-data';
  const STORE = 'verses';

  function openBibleDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: 'ref' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function getCachedVerseCount() {
    const db = await openBibleDB();
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, 'readonly');
      const countReq = tx.objectStore(STORE).count();
      countReq.onsuccess = () => resolve(countReq.result);
      countReq.onerror = () => resolve(0);
    });
  }

  async function saveVersesToDB(verses) {
    const db = await openBibleDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readwrite');
      const store = tx.objectStore(STORE);
      verses.forEach(v => store.put(v));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function loadAllVersesFromDB() {
    const db = await openBibleDB();
    return new Promise((resolve) => {
      const tx = db.transaction(STORE, 'readonly');
      const req = tx.objectStore(STORE).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => resolve([]);
    });
  }

  function setBibleStatus(mode, text) {
    const el = document.getElementById('bible-status');
    el.className = 'bible-status ' + mode;
    el.innerHTML = `<span class="dot"></span>${text}`;
  }

  // Bible is one indivisible index, so it lives in a single worker (index 0)
  // rather than being sharded — the other workers just never get bible data
  // and their bibleFuse stays null, which search-worker.js already handles.
  function loadBibleIntoWorkers(verses) {
    if (searchWorkers.length) searchWorkers[0].postMessage({ type: 'load', name: 'bible', data: verses });
  }

  // Splits source-text rows across the worker pool so every worker owns a
  // disjoint slice of the ~30 collections and searches its slice in
  // parallel with the others. A whole collection (e.g. all of the Qur'an,
  // or all of one Hadith book) always goes to a single worker together —
  // splitting one collection across workers would just mean re-merging its
  // own results, for no benefit. Collections vary hugely in size (the
  // Mahabharata's rows alone average ~7KB each, far above most other
  // texts'), so this bin-packs by total *character count* rather than row
  // count — row count alone under-weighted collections made of few but very
  // long rows, leaving their worker the slowest shard even when every
  // worker had a similar row total. Every worker gets a 'load' call even
  // when its slice is empty, so a previous, larger assignment gets cleared.
  function loadSourceIntoWorkers(rows) {
    if (!searchWorkers.length) return;
    const bySourceId = {};
    rows.forEach(r => { (bySourceId[r.sourceId] = bySourceId[r.sourceId] || []).push(r); });

    const weighOf = (sourceRows) => sourceRows.reduce((sum, r) => sum + (r.text ? r.text.length : 0), 0);
    const buckets = searchWorkers.map(() => ({ rows: [], weight: 0 }));
    Object.values(bySourceId)
      .sort((a, b) => weighOf(b) - weighOf(a)) // largest collections placed first for better packing
      .forEach((sourceRows) => {
        const lightest = buckets.reduce((min, b) => (b.weight < min.weight ? b : min), buckets[0]);
        lightest.rows.push(...sourceRows);
        lightest.weight += weighOf(sourceRows);
      });

    buckets.forEach((bucket, i) => {
      searchWorkers[i].postMessage({ type: 'load', name: 'source', data: bucket.rows });
    });
  }

  function buildBibleIndex(verses) {
    allBibleVerses = verses;
    bibleIndexReady = true;
    buildRealWordSet(verses);
    setBibleStatus('ready', `Full Bible loaded — ${verses.length.toLocaleString()} verses searchable offline`);
    loadBibleIntoWorkers(verses);
    if (searchInput.value.trim()) render();
    renderReadSourcePicker();
  }

  async function downloadFullBible() {
    setBibleStatus('downloading', 'Downloading full Bible for offline use…');
    const flatVerses = [];
    let failedBooks = 0;
    await Promise.all(BIBLE_BOOKS.map(async (b) => {
      try {
        const res = await fetch(`https://cdn.jsdelivr.net/gh/aruljohn/Bible-kjv@a9aa4e55afbb3e095f57e4b14cd1f22c5ee8d7c9/${b}.json`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const book = await res.json();
        (book.chapters || []).forEach(ch => {
          (ch.verses || []).forEach(v => {
            flatVerses.push({ ref: `${book.book} ${ch.chapter}:${v.verse}`, text: v.text });
          });
        });
      } catch (err) {
        failedBooks++;
      }
    }));

    if (flatVerses.length === 0) {
      setBibleStatus('offline-empty', 'Could not download Scripture data — check connection and reload.');
      return;
    }

    try {
      await saveVersesToDB(flatVerses);
    } catch (err) {
      console.warn('TEP: failed to persist Bible verses to IndexedDB:', err);
    }
    buildBibleIndex(flatVerses);
    if (failedBooks > 0) {
      setBibleStatus('ready', `Full Bible loaded — ${flatVerses.length.toLocaleString()} verses searchable (${failedBooks} book${failedBooks === 1 ? '' : 's'} failed to download — try Recheck Sources later)`);
    }
  }

  async function initBibleData() {
    const cachedCount = await getCachedVerseCount();
    if (cachedCount > 0) {
      const verses = await loadAllVersesFromDB();
      buildBibleIndex(verses);
    } else if (navigator.onLine) {
      await downloadFullBible();
    } else {
      setBibleStatus('offline-empty', 'No signal yet — connect once to download the full Bible for offline use.');
    }
  }

  /* ================= Source-text storage / index ================= */

  const SRC_DB_NAME = 'tep-source-texts';
  const SRC_STORE = 'texts';
  const SRC_META_STORE = 'meta';
  const sourceStatus = {}; // id -> 'ready' | 'failed' | 'loading' | 'missing'
  const sourceByteSize = {}; // id -> approx bytes currently stored in IndexedDB

  // Traditions whose texts download automatically on first load. Anything
  // else (future additions — new traditions, alternate-language editions)
  // is opt-in only, downloaded from Settings -> Source Texts.
  const AUTO_DOWNLOAD_TRADITIONS = new Set(['Islam', 'Hinduism', 'Buddhism', 'Catholicism', 'Apocrypha']);

  // User overrides of the default install state, keyed by tradition name:
  // true = keep installed, false = removed from device. Absence means "use
  // the AUTO_DOWNLOAD_TRADITIONS default." Persisted so an uninstall (or an
  // opt-in install) survives a reload instead of silently reverting.
  let traditionOverrides = {};

  function isTraditionWanted(tradition) {
    if (Object.prototype.hasOwnProperty.call(traditionOverrides, tradition)) {
      return traditionOverrides[tradition];
    }
    return AUTO_DOWNLOAD_TRADITIONS.has(tradition);
  }

  async function setTraditionOverride(tradition, wanted) {
    traditionOverrides[tradition] = wanted;
    await setSetting('tradition-overrides', traditionOverrides);
  }

  function formatBytes(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  }

  // Bump this whenever a source's fetch/parse logic changes in a way that
  // could make previously-cached rows wrong or stale (e.g. the Dhammapada
  // fix that stopped picking up raw HTML/placeholder markup). A version
  // bump makes initSourceTexts() wipe IndexedDB and re-fetch everything
  // once, instead of silently keeping old bad data forever.
  const SOURCE_SCHEMA_VERSION = 2;

  function openSourceDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(SRC_DB_NAME, 2);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(SRC_STORE)) {
          const s = db.createObjectStore(SRC_STORE, { keyPath: 'key' });
          s.createIndex('sourceId', 'sourceId');
        }
        if (!db.objectStoreNames.contains(SRC_META_STORE)) {
          db.createObjectStore(SRC_META_STORE, { keyPath: 'key' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function getSourceSchemaVersion() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_META_STORE, 'readonly');
      const req = tx.objectStore(SRC_META_STORE).get('schemaVersion');
      req.onsuccess = () => resolve(req.result ? req.result.value : null);
      req.onerror = () => resolve(null);
    });
  }

  async function setSourceSchemaVersion(version) {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_META_STORE, 'readwrite');
      tx.objectStore(SRC_META_STORE).put({ key: 'schemaVersion', value: version });
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  async function saveSourceRows(sourceId, rows) {
    const db = await openSourceDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      const store = tx.objectStore(SRC_STORE);
      rows.forEach(r => store.put({
        key: `${sourceId}|${r.ref}`,
        sourceId,
        ref: r.ref,
        text: r.text
      }));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function loadAllSourceRows() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_STORE, 'readonly');
      const req = tx.objectStore(SRC_STORE).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => resolve([]);
    });
  }

  async function clearAllSourceRows() {
    const db = await openSourceDB();
    return new Promise((resolve) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      tx.objectStore(SRC_STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
    });
  }

  async function deleteSourceRowsForIds(sourceIds) {
    const db = await openSourceDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(SRC_STORE, 'readwrite');
      const store = tx.objectStore(SRC_STORE);
      const index = store.index('sourceId');
      sourceIds.forEach(id => {
        const cursorReq = index.openKeyCursor(IDBKeyRange.only(id));
        cursorReq.onsuccess = (e) => {
          const cursor = e.target.result;
          if (cursor) {
            store.delete(cursor.primaryKey);
            cursor.continue();
          }
        };
      });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  function buildSourceIndex(rows) {
    allSourceRows = rows;
    sourceIndexReady = rows.length > 0;
    Object.keys(sourceByteSize).forEach(k => delete sourceByteSize[k]);
    rows.forEach(r => {
      sourceByteSize[r.sourceId] = (sourceByteSize[r.sourceId] || 0) + (r.text ? r.text.length : 0) + (r.ref ? r.ref.length : 0);
    });
    loadSourceIntoWorkers(rows); // always runs, even for [] — clears every worker's stale indices after an uninstall
    if (!rows.length) { renderReadSourcePicker(); return; }
    if (searchInput.value.trim()) render();
    renderReadSourcePicker();
  }

  // Records exactly what happened on every fetch attempt, so a failure can be
  // diagnosed instead of guessed at. Keyed by source id.
  const sourceDiagnostics = {};

  function classifyError(err) {
    const msg = String((err && err.message) || err || 'unknown');
    // A network-level fetch rejection (as opposed to an HTTP error status) in a
    // browser almost always means CORS or DNS/offline, not a wrong path.
    if (/Failed to fetch|NetworkError|Load failed/i.test(msg)) {
      return 'BLOCKED — CORS or network (not a bad path)';
    }
    if (/Unexpected token|JSON|SyntaxError/i.test(msg)) {
      return 'NOT JSON — got HTML/other (usually a wrong path)';
    }
    return msg;
  }

  async function fetchSource(source) {
    const attempts = [];
    sourceDiagnostics[source.id] = attempts;

    // Some sources need multiple requests (e.g. one per chapter) and supply
    // their own fetch routine, returning rows directly.
    if (source.fetchRows) {
      try {
        const rows = await source.fetchRows();
        attempts.push({ url: '(multi-request)', result: `${rows ? rows.length : 0} rows` });
        if (!rows || rows.length === 0) throw new Error('no rows returned');
        return rows;
      } catch (err) {
        attempts.push({ url: '(multi-request)', result: classifyError(err) });
        throw err;
      }
    }

    // Resolve candidate URLs (some sources discover their URL dynamically).
    let urls = source.urls || [];
    if (source.discover) {
      try {
        urls = await source.discover();
        attempts.push({ url: '(discovery)', result: `resolved ${urls.length} candidate URL(s)` });
      } catch (err) {
        attempts.push({ url: '(discovery)', result: classifyError(err) });
        throw new Error(`could not resolve source URL: ${classifyError(err)}`);
      }
    }

    if (!urls.length) {
      attempts.push({ url: '(none)', result: 'no candidate URLs' });
      throw new Error('no candidate URLs');
    }

    let lastErr = null;
    for (const url of urls) {
      try {
        const res = await fetch(url);
        if (!res.ok) {
          attempts.push({ url, result: `HTTP ${res.status}` });
          lastErr = new Error(`HTTP ${res.status}`);
          continue;
        }
        const data = await res.json();
        const rows = source.parse(data);
        if (rows.length > 0) {
          attempts.push({ url, result: `OK — ${rows.length} verses` });
          return rows;
        }
        // Reached the file but got nothing usable: show the payload's top-level
        // keys, which is exactly what's needed to fix the parser.
        const shape = data && typeof data === 'object'
          ? Object.keys(data).slice(0, 6).join(', ') || '(empty object)'
          : typeof data;
        attempts.push({ url, result: `parsed 0 rows — payload keys: [${shape}]` });
        lastErr = new Error('parsed 0 rows');
      } catch (err) {
        attempts.push({ url, result: classifyError(err) });
        lastErr = err;
      }
    }
    throw lastErr || new Error('all URLs failed');
  }

  async function downloadSourceTexts(targets) {
    const list = targets || SOURCE_TEXTS;
    const results = await Promise.all(list.map(async (source) => {
      sourceStatus[source.id] = 'loading';
      try {
        const rows = await fetchSource(source);
        await saveSourceRows(source.id, rows);
        sourceStatus[source.id] = 'ready';
        return { id: source.id, count: rows.length };
      } catch (err) {
        // Visible failure, never silent — app keeps working without this source.
        console.warn(`TEP: source "${source.id}" unavailable:`, err.message);
        sourceStatus[source.id] = 'failed';
        return { id: source.id, count: 0, error: err.message };
      }
    }));

    const all = await loadAllSourceRows();
    buildSourceIndex(all);
    renderSourceStatusPanel();
    return results;
  }

  async function initSourceTexts() {
    const storedSchemaVersion = await getSourceSchemaVersion();
    if (storedSchemaVersion !== SOURCE_SCHEMA_VERSION) {
      await clearAllSourceRows();
      await setSourceSchemaVersion(SOURCE_SCHEMA_VERSION);
    }

    traditionOverrides = (await getSetting('tradition-overrides')) || {};

    const cached = await loadAllSourceRows();
    if (cached.length > 0) {
      const present = new Set(cached.map(r => r.sourceId));
      SOURCE_TEXTS.forEach(s => { sourceStatus[s.id] = present.has(s.id) ? 'ready' : 'missing'; });
      buildSourceIndex(cached);
    } else {
      SOURCE_TEXTS.forEach(s => { sourceStatus[s.id] = 'missing'; });
    }
    renderSourceStatusPanel();

    if (navigator.onLine) {
      const toFetch = SOURCE_TEXTS.filter(s => sourceStatus[s.id] !== 'ready' && isTraditionWanted(s.tradition));
      if (toFetch.length) downloadSourceTexts(toFetch);
    }
  }

  async function installTradition(tradition) {
    if (!navigator.onLine) { showToast('You appear to be offline'); return; }
    await setTraditionOverride(tradition, true);
    const targets = SOURCE_TEXTS.filter(s => s.tradition === tradition);
    targets.forEach(s => { sourceStatus[s.id] = 'loading'; });
    renderSourceStatusPanel();
    await downloadSourceTexts(targets);
    const okCount = targets.filter(s => sourceStatus[s.id] === 'ready').length;
    showToast(okCount === targets.length
      ? `${tradition} downloaded — searchable offline`
      : `${tradition}: ${okCount} of ${targets.length} texts downloaded`);
  }

  async function uninstallTradition(tradition) {
    const ids = SOURCE_TEXTS.filter(s => s.tradition === tradition).map(s => s.id);
    // Flip status and re-render immediately, before the async delete
    // resolves — otherwise the "Remove from device" button stays live
    // during the await and a second click re-enters this function for the
    // same tradition (harmless, since the delete is idempotent, but wasteful).
    ids.forEach(id => { sourceStatus[id] = 'removing'; });
    renderSourceStatusPanel();
    await deleteSourceRowsForIds(ids);
    ids.forEach(id => { sourceStatus[id] = 'missing'; delete sourceDiagnostics[id]; });
    await setTraditionOverride(tradition, false);
    const all = await loadAllSourceRows();
    buildSourceIndex(all);
    renderSourceStatusPanel();
    showToast(`${tradition} removed from this device — download it again anytime`);
  }

  // Display order for tradition groups — SOURCE_TEXTS insertion order, deduped.
  function sourceTraditionOrder() {
    const seen = [];
    SOURCE_TEXTS.forEach(s => { if (!seen.includes(s.tradition)) seen.push(s.tradition); });
    return seen;
  }

  function renderSourceStatusPanel() {
    const el = document.getElementById('source-status-list');
    if (!el) return;

    el.innerHTML = sourceTraditionOrder().map(tradition => {
      const sources = SOURCE_TEXTS.filter(s => s.tradition === tradition);
      const wanted = isTraditionWanted(tradition);
      const statuses = sources.map(s => sourceStatus[s.id] || 'missing');
      const readyCount = statuses.filter(st => st === 'ready').length;
      const anyLoading = statuses.includes('loading');
      const anyRemoving = statuses.includes('removing');
      const allReady = readyCount === sources.length;

      let dotClass, summary;
      if (anyRemoving) {
        dotClass = 'downloading'; summary = 'Removing…';
      } else if (anyLoading) {
        dotClass = 'downloading'; summary = 'Downloading…';
      } else if (allReady) {
        dotClass = 'ready'; summary = `Downloaded — ${readyCount} text${readyCount === 1 ? '' : 's'} searchable offline`;
      } else if (readyCount > 0) {
        dotClass = 'offline-empty'; summary = `Partially downloaded — ${readyCount} of ${sources.length} texts`;
      } else {
        dotClass = 'offline-empty'; summary = wanted ? 'Not downloaded yet' : 'Not downloaded — optional';
      }

      const bytes = sources.reduce((sum, s) => sum + (sourceByteSize[s.id] || 0), 0);
      const sizeLabel = bytes > 0 ? ` (${formatBytes(bytes)} on device)` : '';

      const actionBtn = (anyLoading || anyRemoving) ? ''
        : allReady
          ? `<button class="contact-btn source-action-btn" data-action="uninstall" data-tradition="${escapeHtml(tradition)}">Remove from device</button>`
          : `<button class="submit-btn source-action-btn" data-action="install" data-tradition="${escapeHtml(tradition)}">Download</button>`;

      const licenses = [...new Set(sources.map(s => s.license))].join(' · ');

      const filesDetail = sources.map(s => {
        const st = sourceStatus[s.id] || 'missing';
        const label = st === 'ready' ? 'Loaded' : st === 'loading' ? 'Downloading…' : st === 'removing' ? 'Removing…' : st === 'failed' ? 'Unavailable' : 'Not downloaded';
        const attempts = sourceDiagnostics[s.id] || [];
        const diag = attempts.length
          ? `<div class="source-diag">${attempts.map(a => `
               <div class="diag-line">
                 <span class="diag-url">${escapeHtml(a.url.length > 72 ? a.url.slice(0, 69) + '…' : a.url)}</span>
                 <span class="diag-result">${escapeHtml(a.result)}</span>
               </div>`).join('')}</div>`
          : '';
        return `<div class="source-file-row"><span>${escapeHtml(s.label)}</span><span>${label}</span></div>${diag}`;
      }).join('');

      return `
        <div class="source-status-row">
          <div class="source-status-head">
            <p class="bible-status ${dotClass}" style="margin:0;"><span class="dot"></span><strong>${escapeHtml(tradition)}</strong> — ${summary}${sizeLabel}</p>
            ${actionBtn}
          </div>
          <p class="source-license">${escapeHtml(licenses)}</p>
          <details class="source-file-details"><summary>${sources.length} text${sources.length === 1 ? '' : 's'}</summary>${filesDetail}</details>
        </div>`;
    }).join('');

    el.querySelectorAll('[data-action="uninstall"]').forEach(btn => {
      btn.addEventListener('click', () => uninstallTradition(btn.dataset.tradition));
    });
    el.querySelectorAll('[data-action="install"]').forEach(btn => {
      btn.addEventListener('click', () => installTradition(btn.dataset.tradition));
    });
  }

  /* Builds a plain-text report of every fetch attempt — easy to copy and send. */
  function buildDiagnosticReport() {
    const lines = ['TEP source diagnostics', new Date().toISOString(), ''];
    SOURCE_TEXTS.forEach(s => {
      lines.push(`[${(sourceStatus[s.id] || 'missing').toUpperCase()}] ${s.label} (${s.tradition})`);
      const attempts = sourceDiagnostics[s.id] || [];
      if (!attempts.length) lines.push('  (no attempts recorded)');
      attempts.forEach(a => lines.push(`  ${a.url}\n    -> ${a.result}`));
      lines.push('');
    });
    return lines.join('\n');
  }

  async function recheckSources() {
    if (!navigator.onLine) {
      showToast('You appear to be offline');
      return;
    }
    const targets = SOURCE_TEXTS.filter(s => isTraditionWanted(s.tradition));
    targets.forEach(s => { sourceStatus[s.id] = 'loading'; });
    renderSourceStatusPanel();
    await downloadSourceTexts(targets);
    const okCount = targets.filter(s => sourceStatus[s.id] === 'ready').length;
    showToast(`${okCount} of ${targets.length} installed sources loaded`);
  }

  /* Renders matches from other traditions' texts, showing only the text
     tied to whichever religion is selected in the sidebar (or all of them
     when "All" is selected). The sidebar is the single show/hide control. */
  function renderSourceResults(query, sourceResult) {
    const section = document.getElementById('source-results-section');
    const container = document.getElementById('source-results');
    const heading = document.getElementById('source-results-heading');
    if (!section || !container) return;

    if (SOURCE_TEXTS.length === 0) { section.style.display = 'none'; return; }

    // A tradition (e.g. Islam) can now have several source texts (Qur'an
    // plus the six hadith collections), so filtering is by a *list* of
    // matching sources, not a single one — narrowed further to just one
    // specific text when the text-filter dropdown picks one out.
    const matchingSources = activeSourceId
      ? SOURCE_TEXTS.filter(s => s.id === activeSourceId)
      : (activeFilter === 'all' ? SOURCE_TEXTS : SOURCE_TEXTS.filter(s => s.tradition === activeFilter));
    const filteringToUncoveredReligion = activeFilter !== 'all' && matchingSources.length === 0;

    if (heading) {
      heading.textContent = filteringToUncoveredReligion
        ? `From ${activeFilter}'s own text`
        : activeSourceId
          ? `From ${matchingSources[0] ? matchingSources[0].label : activeFilter}`
          : (activeFilter === 'all'
              ? `From other traditions' own texts`
              : `From ${activeFilter}'s own text${matchingSources.length > 1 ? 's' : ''}`);
    }

    // The sidebar has a religion selected that has no primary text loaded
    // (e.g. Atheism, Humanism) — hide the results list and say so plainly.
    if (filteringToUncoveredReligion) {
      section.style.display = 'block';
      container.innerHTML = `<p class="source-tab-note">No primary source text is loaded for ${escapeHtml(activeFilter)} yet. Select "All" or a tradition with a loaded text.</p>`;
      return;
    }

    // A religion is selected but none of its sources have finished loading —
    // say that too, even before anything has been typed.
    const readySources = matchingSources.filter(s => sourceStatus[s.id] === 'ready');
    if (activeFilter !== 'all' && readySources.length === 0) {
      section.style.display = 'block';
      const anyLoading = matchingSources.some(s => sourceStatus[s.id] === 'loading');
      const reason = anyLoading ? 'is still downloading' : "hasn't loaded yet";
      const label = matchingSources.length === 1 ? matchingSources[0].label : `${activeFilter}'s texts`;
      container.innerHTML = `<p class="source-tab-note">${escapeHtml(label)} ${reason}. Check Settings → Source Texts to re-check, or try again once you're online.</p>`;
      return;
    }

    if (!query.trim() || !sourceIndexReady) {
      section.style.display = 'none';
      container.innerHTML = '';
      return;
    }

    // Tradition filtering (by readySources' ids) already happened inside the
    // worker via sourceIdWhitelist, so sourceResult is ready to use as-is.
    if (sourceResult.total === 0) {
      section.style.display = 'block';
      container.innerHTML = `<p class="source-tab-note">No matches for &ldquo;${escapeHtml(query)}&rdquo;${activeFilter !== 'all' ? ' in this text' : ''}.</p>`;
      return;
    }
    const matches = sourceResult.top.slice(0, 25).map(m => m.item);
    section.style.display = 'block';

    // Group by tradition so it reads as "what each religion's own text says".
    const byTradition = {};
    matches.forEach(m => {
      const src = SOURCE_TEXTS.find(s => s.id === m.sourceId);
      const trad = src ? src.tradition : 'Other';
      (byTradition[trad] = byTradition[trad] || []).push(m);
    });

    const countHtml = `<p class="results-count">Results 1-${matches.length} of <strong>${sourceResult.total.toLocaleString()}</strong> for &ldquo;${escapeHtml(query)}&rdquo;</p>`;

    container.innerHTML = countHtml + Object.entries(byTradition).map(([tradition, rows]) => `
      <div class="source-group">
        <div class="source-group-label">${escapeHtml(tradition)}</div>
        ${rows.map(r => `
          <div class="bible-result-item source-item">
            <span class="ref">${escapeHtml(r.ref)}</span>
            <p>"${highlightText(escapeHtml(r.text), query)}"</p>
          </div>
        `).join('')}
      </div>
    `).join('');
  }

  /* ================= Read Full Texts ================= */
  /*
   * Lets a user page through an entire loaded text (KJV Bible, Qur'an,
   * Bhagavad Gita, Dhammapada) chapter by chapter, instead of only ever
   * seeing a search hit or the single verse cited in a case card.
   */

  const READ_SOURCES = [
    { key: 'bible', label: 'Bible (KJV)', tradition: 'Christianity' },
    { key: 'quran', label: "Qur'an", tradition: 'Islam' },
    { key: 'hadith-bukhari', label: 'Sahih al-Bukhari', tradition: 'Islam' },
    { key: 'hadith-muslim', label: 'Sahih Muslim', tradition: 'Islam' },
    { key: 'hadith-abudawud', label: 'Sunan Abu Dawud', tradition: 'Islam' },
    { key: 'hadith-tirmidhi', label: 'Jami at-Tirmidhi', tradition: 'Islam' },
    { key: 'hadith-nasai', label: "Sunan an-Nasa'i", tradition: 'Islam' },
    { key: 'hadith-ibnmajah', label: 'Sunan Ibn Majah', tradition: 'Islam' },
    { key: 'gita', label: 'Bhagavad Gita', tradition: 'Hinduism' },
    { key: 'yoga-sutras', label: 'Yoga Sutras of Patanjali', tradition: 'Hinduism' },
    { key: 'brahma-sutras', label: 'Brahma Sutras', tradition: 'Hinduism' },
    { key: 'upanishads', label: 'The Principal Upanishads', tradition: 'Hinduism' },
    { key: 'ramayana', label: 'The Ramayana', tradition: 'Hinduism' },
    { key: 'vishnu-purana', label: 'Vishnu Purana', tradition: 'Hinduism' },
    { key: 'manusmriti', label: 'Manusmriti (Laws of Manu)', tradition: 'Hinduism' },
    { key: 'mahabharata', label: 'The Mahabharata', tradition: 'Hinduism' },
    { key: 'dhammapada', label: 'Dhammapada', tradition: 'Buddhism' },
    { key: 'digha-nikaya', label: 'Digha Nikaya (Long Discourses)', tradition: 'Buddhism' },
    { key: 'majjhima-nikaya', label: 'Majjhima Nikaya (Middle Discourses)', tradition: 'Buddhism' },
    { key: 'sutta-nipata', label: 'Sutta Nipata', tradition: 'Buddhism' },
    { key: 'udana', label: 'Udana', tradition: 'Buddhism' },
    { key: 'itivuttaka', label: 'Itivuttaka', tradition: 'Buddhism' },
    { key: 'sn-an-highlights', label: 'Selected Suttas (Samyutta & Anguttara)', tradition: 'Buddhism' },
    { key: 'heart-sutra', label: 'Heart Sutra', tradition: 'Buddhism' },
    { key: 'baltimore-catechism', label: 'Baltimore Catechism', tradition: 'Catholicism' },
    { key: 'deuterocanon', label: 'Apocrypha / Deuterocanon', tradition: 'Apocrypha' },
    { key: 'trent-canons', label: 'Council of Trent — Canons', tradition: 'Catholicism' },
    { key: 'rerum-novarum', label: 'Rerum Novarum (1891)', tradition: 'Catholicism' },
    { key: 'ineffabilis-deus', label: 'Ineffabilis Deus (1854)', tradition: 'Catholicism' },
    { key: 'summa-theologica', label: 'Summa Theologica', tradition: 'Catholicism' }
  ];

  // Display order for the religion picker — READ_SOURCES insertion order
  // already follows this, but keep it explicit in case entries get reordered.
  const READ_RELIGION_ORDER = ['Christianity', 'Catholicism', 'Apocrypha', 'Islam', 'Hinduism', 'Buddhism'];

  // Canonical KJV reading order — BIBLE_BOOKS (declared earlier, for CDN
  // fetch filenames) is alphabetical, which would list books completely out
  // of reading order in a book picker.
  const BIBLE_BOOK_ORDER = [
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth",
    "1Samuel","2Samuel","1Kings","2Kings","1Chronicles","2Chronicles","Ezra","Nehemiah",
    "Esther","Job","Psalms","Proverbs","Ecclesiastes","SongofSolomon","Isaiah","Jeremiah",
    "Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah",
    "Nahum","Habakkuk","Zephaniah","Haggai","Zechariah","Malachi",
    "Matthew","Mark","Luke","John","Acts","Romans","1Corinthians","2Corinthians",
    "Galatians","Ephesians","Philippians","Colossians","1Thessalonians","2Thessalonians",
    "1Timothy","2Timothy","Titus","Philemon","Hebrews","James","1Peter","2Peter",
    "1John","2John","3John","Jude","Revelation"
  ];

  let readReligionKey = null; // Selected religion — the dropdown lists this religion's texts
  let readSourceKey = null;
  let readBookKey = null;   // Bible only: selected book name, as it appears in the data
  let readGroupKey = null;  // Bible: selected chapter number; others: surah/chapter/verse-range label

  function readSourceReady(key) {
    if (key === 'bible') return allBibleVerses.length > 0;
    return sourceStatus[key] === 'ready';
  }

  function readSourceRows(key) {
    if (key === 'bible') return allBibleVerses;
    return allSourceRows.filter(r => r.sourceId === key);
  }

  // Pulls a stable, sortable descriptor out of each ref format.
  function parseReadRef(sourceKey, ref) {
    if (sourceKey === 'bible') {
      const m = ref.match(/^(.*)\s(\d+):(\d+)$/);
      if (!m) return null;
      return { book: m[1], chapter: parseInt(m[2], 10), verse: parseInt(m[3], 10) };
    }
    if (sourceKey === 'quran') {
      const m = ref.match(/^Qur'an (\d+):(\d+)$/i);
      if (!m) return null;
      return { group: `Surah ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'gita') {
      const m = ref.match(/^Bhagavad Gita (\d+):(\d+)$/);
      if (m) return { group: `Chapter ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
      return { group: 'Other', chapter: 999, verse: 0 };
    }
    if (sourceKey === 'dhammapada') {
      const m = ref.match(/^Dhammapada (\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `Verses ${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey === 'manusmriti') {
      const m = ref.match(/^Manusmriti (\d+):(\d+)/);
      if (!m) return null;
      return { group: `Chapter ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'yoga-sutras') {
      const m = ref.match(/^Yoga Sutras (\d+)\.(\d+)$/);
      if (!m) return null;
      const PADA_NAMES = ['', 'Samadhi Pada', 'Sadhana Pada', 'Vibhuti Pada', 'Kaivalya Pada'];
      return { group: PADA_NAMES[parseInt(m[1], 10)] || `Pada ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'mahabharata') {
      const m = ref.match(/^Mahabharata, (\w+) Parva, Section (\d+)$/);
      if (!m) return null;
      const PARVA_ORDER = [
        'Adi', 'Sabha', 'Vana', 'Virata', 'Udyoga', 'Bhishma', 'Drona', 'Karna',
        'Shalya', 'Sauptika', 'Stri', 'Shanti', 'Anushasana', 'Ashvamedhika',
        'Ashramavasika', 'Mausala', 'Mahaprasthanika', 'Svargarohana'
      ];
      const idx = PARVA_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Parva`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'upanishads') {
      const m = ref.match(/^(.+) Upanishad (\d+)$/);
      if (!m) return null;
      const UPANISHAD_ORDER = [
        'Brihadaranyaka', 'Chandogya', 'Taittiriya', 'Aitareya', 'Kaushitaki',
        'Kena', 'Katha', 'Isha', 'Mundaka', 'Prashna', 'Mandukya',
        'Svetasvatara', 'Maitri'
      ];
      const idx = UPANISHAD_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Upanishad`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'ramayana') {
      const m = ref.match(/^Ramayana, (\w+) Kanda, Canto (\d+)$/);
      if (!m) return null;
      const KANDA_ORDER = ['Bala', 'Ayodhya', 'Aranya', 'Kishkindha', 'Sundara', 'Yuddha'];
      const idx = KANDA_ORDER.indexOf(m[1]);
      return { group: `${m[1]} Kanda`, chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'vishnu-purana') {
      const m = ref.match(/^Vishnu Purana (\d+)\.(\d+)$/);
      if (!m) return null;
      return { group: `Book ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'brahma-sutras') {
      const m = ref.match(/^Brahma Sutras (\d+)\.(\d+)\.(\d+)$/);
      if (!m) return null;
      const adhyaya = parseInt(m[1], 10), pada = parseInt(m[2], 10);
      return {
        group: `Adhyaya ${m[1]}, Pada ${m[2]}`,
        chapter: adhyaya * 10 + pada,
        verse: parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'digha-nikaya' || sourceKey === 'majjhima-nikaya') {
      const label = sourceKey === 'digha-nikaya' ? 'Digha Nikaya' : 'Majjhima Nikaya';
      const titles = sourceKey === 'digha-nikaya' ? DN_TITLES : MN_TITLES;
      const m = ref.match(new RegExp(`^${label} (\\d+)\\.(\\d+)$`));
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const title = titles[n] || '';
      return { group: `${n}. ${title}`, chapter: n, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'heart-sutra') {
      const m = ref.match(/^Heart Sutra \((\w+) Recension\) (\d+)$/);
      if (!m) return null;
      return { group: `${m[1]} Recension`, chapter: m[1] === 'Smaller' ? 1 : 2, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'sn-an-highlights') {
      const m = ref.match(/^(.+) (\d+)$/);
      if (!m) return null;
      const idx = SN_AN_HIGHLIGHTS.findIndex(t => t.label === m[1]);
      return { group: m[1], chapter: idx === -1 ? 999 : idx, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'sutta-nipata') {
      const m = ref.match(/^Sutta Nipata (\d+)\.(\d+)$/);
      if (!m) return null;
      const SNP_VAGGA_NAMES = ['', 'Uragavagga (Serpent)', 'Culavagga (Minor Chapter)', 'Mahavagga (Great Chapter)', 'Atthakavagga (Chapter of Eights)', 'Parayanavagga (The Way to the Far Shore)'];
      const v = parseInt(m[1], 10);
      return { group: SNP_VAGGA_NAMES[v] || `Vagga ${v}`, chapter: v, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'udana') {
      const m = ref.match(/^Udana (\d+)\.(\d+)$/);
      if (!m) return null;
      const UD_VAGGA_NAMES = ['', 'Bodhivagga', 'Mucalindavagga', 'Nandavagga', 'Meghiyavagga', 'Sonavagga', 'Jaccandhavagga', 'Culavagga', 'Pataligamiyavagga'];
      const v = parseInt(m[1], 10);
      return { group: UD_VAGGA_NAMES[v] || `Vagga ${v}`, chapter: v, verse: parseInt(m[2], 10) };
    }
    if (sourceKey === 'itivuttaka') {
      const m = ref.match(/^Itivuttaka (\d+)$/);
      if (!m) return null;
      const ITI_VAGGA_BOUNDS = [10, 20, 27, 37, 49, 59, 69, 79, 89, 99, 112];
      const n = parseInt(m[1], 10);
      let vagga = ITI_VAGGA_BOUNDS.length;
      for (let v = 0; v < ITI_VAGGA_BOUNDS.length; v++) {
        if (n <= ITI_VAGGA_BOUNDS[v]) { vagga = v + 1; break; }
      }
      return { group: `Vagga ${vagga}`, chapter: vagga, verse: n };
    }
    if (sourceKey === 'rerum-novarum' || sourceKey === 'ineffabilis-deus') {
      const m = ref.match(/para\.\s*(\d+)$/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 10) * 10 + 1;
      return { group: `¶${start}–${start + 9}`, chapter: start, verse: n };
    }
    if (sourceKey === 'summa-theologica') {
      const m = ref.match(/^Summa Theologica, ([IVX-]+), Q\.\s*(\d+), Art\.\s*(\d+)$/);
      if (!m) return null;
      const SUMMA_PART_ORDER = ['I', 'I-II', 'II-II', 'III'];
      const partIdx = SUMMA_PART_ORDER.indexOf(m[1]);
      return {
        group: `Part ${m[1]}`,
        chapter: partIdx === -1 ? 999 : partIdx,
        verse: parseInt(m[2], 10) * 10000 + parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'trent-canons') {
      const m = ref.match(/^Council of Trent, (.+), Canon ([IVXLCDM]+)$/);
      if (!m) return null;
      const TRENT_GROUP_ORDER = [
        'Session the Sixth (On Justification)',
        'Session the Seventh (On The Sacraments In General)',
        'Session the Seventh (On Baptism)',
        'Session the Seventh (On Confirmation)',
        'Session the Thirteenth (On The Most Holy Sacrament Of The Eucharist)',
        'Session the Fourteenth (On The Most Holy Sacrament Of Penance)',
        'Session the Fourteenth (On The Sacrament Of Extreme Unction)',
        'Session the Twenty-First (On Communion Under Both Species, And On The Communion Of Infants)',
        'Session the Twenty-Second (On The Sacrifice Of The Mass)',
        'Session the Twenty-Third (On The Sacrament Of Order)',
        'Session the Twenty-Fourth (On The Sacrament Of Matrimony)'
      ];
      const ROMAN = { I: 1, V: 5, X: 10, L: 50, C: 100, D: 500, M: 1000 };
      const toInt = (r) => {
        let n = 0;
        for (let i = 0; i < r.length; i++) {
          const cur = ROMAN[r[i]], next = ROMAN[r[i + 1]];
          n += (next && cur < next) ? -cur : cur;
        }
        return n;
      };
      const groupIdx = TRENT_GROUP_ORDER.indexOf(m[1]);
      return { group: m[1], chapter: groupIdx === -1 ? 999 : groupIdx, verse: toInt(m[2]) };
    }
    if (sourceKey === 'deuterocanon') {
      const m = ref.match(/^(.*)\s(\d+):(\d+)$/);
      if (!m) return null;
      // Grouped by whole book (not book+chapter, unlike Bible) since each
      // book is its own "Read Full Text" group here. Two consequences:
      //  - `chapter` becomes canonical book order, so groups sort correctly
      //    instead of tying (every book's own chapter count restarts at 1).
      //  - `verse` becomes a chapter*1000+verse composite, so verses within
      //    a group still sort correctly — renderReadVerses() only sorts by
      //    `verse`, which elsewhere is always scoped to a single chapter.
      const DEUTERO_ORDER = ['Tobias', 'Judith', 'Wisdom', 'Ecclesiasticus', 'Baruch', '1 Machabees', '2 Machabees'];
      const bookIdx = DEUTERO_ORDER.indexOf(m[1]);
      return {
        group: m[1],
        book: m[1],
        chapter: bookIdx === -1 ? 999 : bookIdx,
        verse: parseInt(m[2], 10) * 1000 + parseInt(m[3], 10)
      };
    }
    if (sourceKey === 'baltimore-catechism') {
      const m = ref.match(/Q\.\s*(\d+)/);
      if (!m) return null;
      const n = parseInt(m[1], 10);
      const start = Math.floor((n - 1) / 50) * 50 + 1;
      return { group: `Q. ${start}–${start + 49}`, chapter: start, verse: n };
    }
    if (sourceKey.startsWith('hadith-')) {
      // Ref format is "<Collection name> <book>:<hadith>", e.g. "Sahih al-Bukhari 12:34".
      const m = ref.match(/(\d+):(\d+)$/);
      if (!m) return { group: 'Other', chapter: 999, verse: 0 };
      return { group: `Book ${m[1]}`, chapter: parseInt(m[1], 10), verse: parseInt(m[2], 10) };
    }
    return null;
  }

  function readBookOrder() {
    const byNorm = {};
    allBibleVerses.forEach(v => {
      const parsed = parseReadRef('bible', v.ref);
      if (!parsed) return;
      const norm = parsed.book.replace(/\s+/g, '').toLowerCase();
      if (!byNorm[norm]) byNorm[norm] = parsed.book;
    });
    const ordered = BIBLE_BOOK_ORDER.map(b => byNorm[b.toLowerCase()]).filter(Boolean);
    // Anything present in the data but not recognized (unexpected book name) still shows up, at the end.
    const known = new Set(ordered);
    Object.values(byNorm).forEach(b => { if (!known.has(b)) ordered.push(b); });
    return ordered;
  }

  function renderReadSourcePicker() {
    const picker = document.getElementById('read-source-picker');
    if (!picker) return;

    const religions = [...new Set(READ_SOURCES.map(s => s.tradition))];
    religions.sort((a, b) => {
      const ai = READ_RELIGION_ORDER.indexOf(a), bi = READ_RELIGION_ORDER.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });

    // Default to the first religion so the page never opens on an empty picker.
    if (!readReligionKey || !religions.includes(readReligionKey)) {
      readReligionKey = religions[0] || null;
    }

    picker.innerHTML = religions.map(r => {
      const activeCls = r === readReligionKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-religion="${escapeHtml(r)}">${escapeHtml(r)}</button>`;
    }).join('');

    picker.querySelectorAll('[data-read-religion]').forEach(btn => {
      btn.addEventListener('click', () => {
        readReligionKey = btn.dataset.readReligion;
        readSourceKey = null;
        readBookKey = null;
        readGroupKey = null;
        renderReadSourcePicker();
      });
    });

    renderReadTextDropdown();
  }

  function renderReadTextDropdown() {
    const container = document.getElementById('read-text-dropdown');
    if (!container) return;
    if (!readReligionKey) { container.innerHTML = ''; return; }

    const texts = READ_SOURCES.filter(s => s.tradition === readReligionKey);
    if (!texts.length) { container.innerHTML = ''; return; }

    // Keep the current selection if it's still one of this religion's texts;
    // otherwise default to the first ready one (or just the first, if none are).
    if (!readSourceKey || !texts.some(t => t.key === readSourceKey)) {
      const firstReady = texts.find(t => readSourceReady(t.key));
      readSourceKey = (firstReady || texts[0]).key;
      readBookKey = null;
      readGroupKey = null;
    }

    container.innerHTML = `
      <select class="read-text-select" id="read-text-select">
        ${texts.map(t => {
          const ready = readSourceReady(t.key);
          const selected = t.key === readSourceKey ? 'selected' : '';
          return `<option value="${t.key}" ${selected} ${ready ? '' : 'disabled'}>${escapeHtml(t.label)}${ready ? '' : ' (not loaded)'}</option>`;
        }).join('')}
      </select>
    `;

    document.getElementById('read-text-select').addEventListener('change', (e) => {
      readSourceKey = e.target.value;
      readBookKey = null;
      readGroupKey = null;
      document.getElementById('read-verses').innerHTML = '';
      renderReadChapterPicker();
    });

    renderReadChapterPicker();
  }

  function renderReadChapterPicker() {
    const container = document.getElementById('read-chapter-picker');
    if (!container) return;
    if (!readSourceKey) { container.innerHTML = ''; return; }

    if (!readSourceReady(readSourceKey)) {
      container.innerHTML = `<p class="read-empty">This text hasn't finished loading yet. Check Settings → Source Texts, or try again once you're online.</p>`;
      document.getElementById('read-verses').innerHTML = '';
      return;
    }

    if (readSourceKey === 'bible') {
      renderReadBibleBookAndChapterPicker(container);
    } else {
      renderReadSimpleChapterPicker(container);
    }
  }

  function renderReadBibleBookAndChapterPicker(container) {
    const books = readBookOrder();
    if (!books.length) {
      container.innerHTML = `<p class="read-empty">No Bible books found yet.</p>`;
      return;
    }

    const bookButtons = `<div class="read-row read-chapter-row">${books.map(b => {
      const activeCls = b === readBookKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-book="${escapeHtml(b)}">${escapeHtml(b)}</button>`;
    }).join('')}</div>`;

    let chapterButtons = '';
    if (readBookKey) {
      const chapters = [...new Set(
        allBibleVerses
          .map(v => parseReadRef('bible', v.ref))
          .filter(p => p && p.book === readBookKey)
          .map(p => p.chapter)
      )].sort((a, b) => a - b);
      chapterButtons = `<div class="read-row read-chapter-row" style="margin-top:10px;">${chapters.map(c => {
        const activeCls = String(c) === readGroupKey ? ' active' : '';
        return `<button class="read-btn${activeCls}" data-read-group="${c}">${c}</button>`;
      }).join('')}</div>`;
    }

    container.innerHTML = bookButtons + chapterButtons;

    container.querySelectorAll('[data-read-book]').forEach(btn => {
      btn.addEventListener('click', () => {
        readBookKey = btn.dataset.readBook;
        readGroupKey = null;
        document.getElementById('read-verses').innerHTML = '';
        renderReadChapterPicker();
      });
    });
    container.querySelectorAll('[data-read-group]').forEach(btn => {
      btn.addEventListener('click', () => {
        readGroupKey = btn.dataset.readGroup;
        renderReadChapterPicker();
        renderReadVerses();
      });
    });
  }

  function renderReadSimpleChapterPicker(container) {
    const rows = readSourceRows(readSourceKey);
    const groups = [];
    const seen = new Set();
    rows.forEach(r => {
      const parsed = parseReadRef(readSourceKey, r.ref);
      if (!parsed || seen.has(parsed.group)) return;
      seen.add(parsed.group);
      groups.push(parsed);
    });
    groups.sort((a, b) => a.chapter - b.chapter);

    if (!groups.length) {
      container.innerHTML = `<p class="read-empty">No chapters found for this text yet.</p>`;
      return;
    }

    container.innerHTML = `<div class="read-row read-chapter-row">${groups.map(g => {
      const activeCls = g.group === readGroupKey ? ' active' : '';
      return `<button class="read-btn${activeCls}" data-read-group="${escapeHtml(g.group)}">${escapeHtml(g.group)}</button>`;
    }).join('')}</div>`;

    container.querySelectorAll('[data-read-group]').forEach(btn => {
      btn.addEventListener('click', () => {
        readGroupKey = btn.dataset.readGroup;
        renderReadChapterPicker();
        renderReadVerses();
      });
    });

    if (readGroupKey && seen.has(readGroupKey)) renderReadVerses();
  }

  function renderReadVerses() {
    const container = document.getElementById('read-verses');
    if (!container || !readSourceKey || !readGroupKey) return;

    let rows;
    let heading;
    if (readSourceKey === 'bible') {
      rows = allBibleVerses
        .map(r => ({ row: r, parsed: parseReadRef('bible', r.ref) }))
        .filter(x => x.parsed && x.parsed.book === readBookKey && String(x.parsed.chapter) === readGroupKey)
        .sort((a, b) => a.parsed.verse - b.parsed.verse);
      heading = `${readBookKey} ${readGroupKey}`;
    } else {
      rows = readSourceRows(readSourceKey)
        .map(r => ({ row: r, parsed: parseReadRef(readSourceKey, r.ref) }))
        .filter(x => x.parsed && x.parsed.group === readGroupKey)
        .sort((a, b) => a.parsed.verse - b.parsed.verse);
      heading = readGroupKey;
    }

    if (!rows.length) {
      container.innerHTML = `<p class="read-empty">Nothing found in this chapter.</p>`;
      return;
    }

    container.innerHTML = `
      <p class="read-verses-heading">${escapeHtml(heading)} — ${rows.length} verse${rows.length === 1 ? '' : 's'}</p>
      ${rows.map(x => `
        <div class="bible-result-item">
          <span class="ref">${escapeHtml(x.row.ref)}</span>
          <p>"${escapeHtml(x.row.text)}"</p>
        </div>
      `).join('')}
    `;
  }

  window.addEventListener('online', () => {
    getCachedVerseCount().then(count => {
      if (count === 0) downloadFullBible();
    });
    loadAllSourceRows().then(rows => {
      const present = new Set(rows.map(r => r.sourceId));
      if (SOURCE_TEXTS.some(s => !present.has(s.id))) downloadSourceTexts();
    });
  });

  const BIBLE_RESULTS_LIMIT = 25;

  function renderBibleResults(query, bibleResult) {
    const section = document.getElementById('bible-results-section');
    const container = document.getElementById('bible-results');
    if (!query.trim() || bibleResult.total === 0) {
      section.style.display = 'none';
      container.innerHTML = '';
      return;
    }
    const matches = bibleResult.top.slice(0, BIBLE_RESULTS_LIMIT);
    section.style.display = 'block';

    const countHtml = `<p class="results-count">Results 1-${matches.length} of <strong>${bibleResult.total.toLocaleString()}</strong> for &ldquo;${escapeHtml(query)}&rdquo;</p>`;

    container.innerHTML = countHtml + matches.map(m => `
      <div class="bible-result-item">
        <a class="ref" href="${bibleLink(m.item.ref)}" target="_blank" rel="noopener noreferrer">${m.item.ref} ↗</a>
        <p>"${highlightText(escapeHtml(m.item.text), query)}"</p>
      </div>
    `).join('');
  }

  // Bible verse search is now triggered from within render() using the corrected query

  render();
  initBibleData();
  initSourceTexts();

  /* ================= App data layer (bookmarks / search log / settings) ================= */

  const APP_DB_NAME = 'tep-app-data';

  function openAppDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(APP_DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains('bookmarks')) db.createObjectStore('bookmarks', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('searchLog')) {
          const s = db.createObjectStore('searchLog', { keyPath: 'auto', autoIncrement: true });
          s.createIndex('timestamp', 'timestamp');
        }
        if (!db.objectStoreNames.contains('settings')) db.createObjectStore('settings', { keyPath: 'key' });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function entryId(entry) {
    return (entry.religion + '|' + entry.claim).toLowerCase().replace(/[^a-z0-9]+/g, '-');
  }

  async function loadBookmarkIds() {
    const db = await openAppDB();
    return new Promise((resolve) => {
      const tx = db.transaction('bookmarks', 'readonly');
      const req = tx.objectStore('bookmarks').getAllKeys();
      req.onsuccess = () => { bookmarkIds = new Set(req.result); resolve(); };
      req.onerror = () => resolve();
    });
  }

  async function toggleBookmark(entry) {
    const id = entryId(entry);
    const db = await openAppDB();
    const tx = db.transaction('bookmarks', 'readwrite');
    const store = tx.objectStore('bookmarks');
    if (bookmarkIds.has(id)) {
      store.delete(id);
      bookmarkIds.delete(id);
    } else {
      store.add({ id, religion: entry.religion, claim: entry.claim, savedAt: Date.now() });
      bookmarkIds.add(id);
    }
    tx.oncomplete = () => {
      render();
      const bookmarksPage = document.getElementById('page-bookmarks');
      if (bookmarksPage && bookmarksPage.style.display !== 'none') renderBookmarksPage();
    };
  }

  async function logSearchHit(id) {
    try {
      const db = await openAppDB();
      const tx = db.transaction('searchLog', 'readwrite');
      tx.objectStore('searchLog').add({ id, timestamp: Date.now() });
    } catch (e) { /* non-critical */ }
  }

  async function getTopSearchedPool(limit = 20) {
    const db = await openAppDB();
    const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
    return new Promise((resolve) => {
      const tx = db.transaction('searchLog', 'readonly');
      const req = tx.objectStore('searchLog').getAll();
      req.onsuccess = () => {
        const counts = {};
        (req.result || []).forEach(row => {
          if (row.timestamp >= thirtyDaysAgo) counts[row.id] = (counts[row.id] || 0) + 1;
        });
        const pool = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, limit).map(([id]) => id);
        resolve(pool);
      };
      req.onerror = () => resolve([]);
    });
  }

  async function getSetting(key) {
    const db = await openAppDB();
    return new Promise((resolve) => {
      const tx = db.transaction('settings', 'readonly');
      const req = tx.objectStore('settings').get(key);
      req.onsuccess = () => resolve(req.result ? req.result.value : null);
      req.onerror = () => resolve(null);
    });
  }

  async function setSetting(key, value) {
    const db = await openAppDB();
    const tx = db.transaction('settings', 'readwrite');
    tx.objectStore('settings').put({ key, value });
  }

  /* ================= Toast ================= */

  function showToast(message, duration = 3000) {
    const toast = document.createElement('div');
    toast.className = 'tep-toast';
    toast.textContent = message;
    document.body.appendChild(toast);
    requestAnimationFrame(() => { toast.style.opacity = '1'; toast.style.transform = 'translateX(-50%) translateY(0)'; });
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(-50%) translateY(20px)';
      setTimeout(() => toast.remove(), 300);
    }, duration);
  }

  /* ================= Share ================= */

  async function shareEntry(entry) {
    const shareData = {
      title: 'The Elijah Project',
      text: `${entry.claim} (${entry.religion}) — see how Scripture responds, in The Elijah Project.`,
      url: window.location.href
    };
    if (navigator.share) {
      try { await navigator.share(shareData); } catch (e) { /* user cancelled */ }
    } else if (navigator.clipboard) {
      await navigator.clipboard.writeText(`${shareData.text} ${shareData.url}`);
      showToast('Link copied');
    }
  }

  /* ================= Drawer navigation ================= */

  const drawer = document.getElementById('drawer');
  const drawerOverlay = document.getElementById('drawer-overlay');
  const menuOpenBtn = document.getElementById('menu-open');
  const drawerCloseBtn = document.getElementById('drawer-close');
  const drawerLinks = document.querySelectorAll('.drawer-link');
  const PAGES = ['home', 'bookmarks', 'read', 'mission', 'help', 'settings', 'sources', 'contact', 'legal'];

  function openDrawer() { drawer.classList.add('open'); drawerOverlay.classList.add('open'); }
  function closeDrawer() { drawer.classList.remove('open'); drawerOverlay.classList.remove('open'); }
  menuOpenBtn.addEventListener('click', openDrawer);
  drawerCloseBtn.addEventListener('click', closeDrawer);
  drawerOverlay.addEventListener('click', closeDrawer);

  function goToPage(page) {
    PAGES.forEach(p => {
      const el = document.getElementById('page-' + p);
      if (el) el.style.display = (p === page) ? (p === 'home' ? 'block' : 'block') : 'none';
    });
    drawerLinks.forEach(l => l.classList.toggle('active', l.dataset.page === page));
    closeDrawer();
    window.scrollTo(0, 0);
    if (page === 'bookmarks') renderBookmarksPage();
    if (page === 'read') renderReadSourcePicker();
  }

  document.querySelectorAll('[data-page]').forEach(el => {
    el.addEventListener('click', () => goToPage(el.dataset.page));
  });

  /* ================= Bookmarks page ================= */

  function renderBookmarksPage() {
    const list = document.getElementById('bookmarks-list');
    const empty = document.getElementById('bookmarks-empty');
    const bookmarkedEntries = ENTRIES.filter(e => bookmarkIds.has(entryId(e)));
    list.innerHTML = '';
    empty.style.display = bookmarkedEntries.length === 0 ? 'block' : 'none';
    bookmarkedEntries.forEach(entry => {
      list.appendChild(buildEntryCard(entry));
    });
  }

  /* ================= Argument of the Day ================= */

  let aotdEntry = null;

  async function renderArgumentOfTheDay() {
    if (ENTRIES.length === 0) return;
    const today = new Date().toISOString().split('T')[0];
    const cachedDate = await getSetting('aotd-date');
    const cachedId = await getSetting('aotd-id');
    let pickId;

    if (cachedDate === today && cachedId) {
      pickId = cachedId;
    } else {
      let pool = await getTopSearchedPool(20);
      if (pool.length < 5) pool = ENTRIES.map(entryId);
      const seed = today.split('-').reduce((acc, n) => acc + parseInt(n, 10), 0);
      pickId = pool[seed % pool.length];
      await setSetting('aotd-date', today);
      await setSetting('aotd-id', pickId);
    }

    aotdEntry = ENTRIES.find(e => entryId(e) === pickId);
    if (!aotdEntry) return;
    document.getElementById('aotd-claim').textContent = aotdEntry.claim;
    document.getElementById('aotd-religion').textContent = aotdEntry.religion;
    document.getElementById('aotd-widget').style.display = 'block';
    document.getElementById('aotd-detail').innerHTML = '';
    document.getElementById('aotd-detail').style.display = 'none';
    document.getElementById('aotd-toggle').textContent = '+';
  }

  document.querySelector('#aotd-widget .aotd-top').addEventListener('click', () => {
    const detail = document.getElementById('aotd-detail');
    const toggle = document.getElementById('aotd-toggle');
    const isOpen = detail.style.display === 'block';
    if (isOpen) {
      detail.style.display = 'none';
      toggle.textContent = '+';
    } else {
      if (aotdEntry && !detail.dataset.built) {
        detail.appendChild(buildEntryCard(aotdEntry));
        detail.dataset.built = '1';
      }
      detail.style.display = 'block';
      toggle.textContent = '−';
    }
  });

  /* ================= Theme settings ================= */

  const themeSwatches = document.querySelectorAll('.theme-swatch');
  function applyTheme(theme) {
    if (theme === 'default') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', theme);
    }
    themeSwatches.forEach(sw => sw.classList.toggle('active', sw.dataset.theme === theme));
  }
  themeSwatches.forEach(sw => {
    sw.addEventListener('click', () => {
      applyTheme(sw.dataset.theme);
      setSetting('theme', sw.dataset.theme);
    });
  });

  /* ================= Contact form ================= */

  const contactForm = document.getElementById('contact-form');
  const contactType = document.getElementById('contact-type');
  const contactDeviceField = document.getElementById('contact-device-field');

  contactType.addEventListener('change', () => {
    contactDeviceField.style.display = contactType.value === 'Bug Report' ? 'block' : 'none';
  });

  contactForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const type = contactType.value;
    const message = document.getElementById('contact-message').value.trim();
    const device = document.getElementById('contact-device').value.trim();
    const replyEmail = document.getElementById('contact-email').value.trim();

    if (!message) {
      showToast('Add a message before posting');
      return;
    }

    let body = message;
    if (type === 'Bug Report' && device) body = `Device/Browser: ${device}\n\n${message}`;
    if (replyEmail) body += `\n\nReply to: ${replyEmail}`;

    const mailto = `mailto:contact@tep-app.com?subject=${encodeURIComponent('TEP ' + type)}&body=${encodeURIComponent(body)}`;
    window.location.href = mailto;
    showToast('Opening your email app…');
    contactForm.reset();
    contactDeviceField.style.display = 'none';
  });

  /* ================= Source diagnostics buttons ================= */

  const recheckBtn = document.getElementById('recheck-sources');
  if (recheckBtn) recheckBtn.addEventListener('click', () => recheckSources());

  const copyDiagBtn = document.getElementById('copy-diag');
  if (copyDiagBtn) {
    copyDiagBtn.addEventListener('click', async () => {
      const report = buildDiagnosticReport();
      try {
        await navigator.clipboard.writeText(report);
        showToast('Diagnostic report copied');
      } catch (err) {
        console.log(report);
        showToast('Copy failed — report printed to console');
      }
    });
  }

  /* ================= Donate button ================= */

  document.getElementById('donate-btn').addEventListener('click', () => {
    showToast('Donations coming soon');
  });

  /* ================= FAQ ================= */

  const FAQ_DATA = [
    { q: "Do I need an account to use TEP?", a: "No. TEP has no login, no accounts, and no user data collection. Everything you save (bookmarks) stays on your device." },
    { q: "Does TEP work without internet?", a: "Yes. All content, including the full KJV Bible, is downloaded to your device the first time you open the app with a connection. Full search, filters, and bookmarks all work offline afterward." },
    { q: "How does content get updated?", a: "When you're back online, TEP automatically checks for updates in the background and downloads only what's changed. A small banner lets you know when new content has been added." },
    { q: "Where do the source texts come from?", a: "Original documents are pulled from public domain sources and open licenses where available. Each entry cites its source so you can verify it independently." },
    { q: "Why does TEP include claims from other religions?", a: "You can't address what you don't accurately understand. TEP presents each tradition's actual claims alongside a biblical response, rather than a strawman version of what they believe." },
    { q: "How is 'Argument of the Day' chosen?", a: "It's pulled from the most-searched topics over the past month, so it reflects what people are actually asking about, not a fixed editorial list." },
    { q: "I found an error in an entry. What do I do?", a: "Use the Report a Bug button in Contact. Include the entry name and what's incorrect, and it'll be reviewed for correction." }
  ];

  function renderFAQ() {
    const list = document.getElementById('faq-list');
    list.innerHTML = '';
    FAQ_DATA.forEach(({ q, a }) => {
      const item = document.createElement('div');
      item.className = 'faq-item';
      item.innerHTML = `<div class="faq-q">${q}<span class="plus">+</span></div><div class="faq-a">${a}</div>`;
      item.addEventListener('click', () => item.classList.toggle('open'));
      list.appendChild(item);
    });
  }

  /* ================= Init app data ================= */

  async function initAppData() {
    await loadBookmarkIds();
    render();
    const savedTheme = await getSetting('theme');
    if (savedTheme) applyTheme(savedTheme);
    renderFAQ();
    renderArgumentOfTheDay();
  }

  initAppData();
